//! Read and write access to persisted viewed state.
//!
//! The UI sends [`SeenCommand`] values through the normal Overlord inbox
//! (`GLOBALS.to_overlord`). The Overlord accumulates them in [`SeenWork`] and
//! runs at most one blocking write batch at a time. When that batch settles it
//! publishes a [`SeenWriteResult`] so the UI can reconcile optimistic state.
//!
//! There is deliberately no separate mailbox or admission lane here: the inbox
//! is the existing unbounded channel, and the only bound is "one write batch in
//! flight".

use crate::error::Error;
use crate::storage::Storage;
use crate::GLOBALS;
use nostr_types::Id;
use std::collections::HashSet;
use std::time::Duration;
use tokio::task::{self, JoinHandle};

/// Look up persisted viewed state. Pending UI writes are not included.
pub fn is_viewed(id: Id) -> Result<bool, Error> {
    is_viewed_in(GLOBALS.db(), id)
}

pub(crate) fn is_viewed_in(storage: &Storage, id: Id) -> Result<bool, Error> {
    storage.is_event_viewed(id)
}

/// A typed seen (viewed) operation sent from the UI to the Overlord.
#[derive(Debug, Clone)]
pub enum SeenCommand {
    /// Mark one event viewed (note hover).
    Event(Id),
    /// Mark exactly these events viewed (feed "mark all read").
    Events(Vec<Id>),
    /// Mark all direct messages read (DM list button). The Overlord reuses
    /// [`Storage::mark_all_dms_read`], so the scan behavior is unchanged.
    AllDms,
}

/// Result of one persisted seen-write batch, published for the UI to reconcile
/// its optimistic state.
///
/// The explicit-ID transaction and the mark-all-DMs scan are independent: one
/// can fail while the other succeeds. They therefore carry separate errors.
#[derive(Debug, Clone, Default)]
pub struct SeenWriteResult {
    /// Ids attempted by the explicit-ID transaction (never the DM scan).
    pub ids: Vec<Id>,
    /// Error for the explicit-ID transaction, if it failed.
    pub ids_error: Option<String>,
    /// Whether this batch also ran the mark-all-DMs scan.
    pub all_dms: bool,
    /// Error for the mark-all-DMs scan, if it failed.
    pub all_dms_error: Option<String>,
}

/// Per-operation errors from one blocking write batch.
///
/// Kept separate from [`SeenWriteResult`] so the Overlord can keep batch
/// identity (ids and the DM flag) outside the blocking task. If the task itself
/// panics, the join error is attributed to whichever operations were requested.
#[derive(Debug, Clone, Default)]
struct SeenWriteErrors {
    ids_error: Option<String>,
    all_dms_error: Option<String>,
}

impl SeenWriteErrors {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.ids_error.is_none() && self.all_dms_error.is_none()
    }
}

/// Seen work accumulated from the Overlord inbox but not yet written.
///
/// A set deduplicates repeated hovers and repeated ids within a feed batch. A
/// boolean coalesces any number of queued mark-all-DMs requests into one scan.
#[derive(Debug, Default)]
struct SeenWork {
    ids: HashSet<Id>,
    all_dms: bool,
}

impl SeenWork {
    fn add(&mut self, command: SeenCommand) {
        match command {
            SeenCommand::Event(id) => {
                self.ids.insert(id);
            }
            SeenCommand::Events(ids) => {
                self.ids.extend(ids);
            }
            SeenCommand::AllDms => {
                self.all_dms = true;
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ids.is_empty() && !self.all_dms
    }

    /// Drain the accumulated work into a single write batch.
    fn take(&mut self) -> (Vec<Id>, bool) {
        (
            self.ids.drain().collect(),
            std::mem::take(&mut self.all_dms),
        )
    }
}

#[derive(Default)]
pub(crate) struct SeenWriter {
    pending: SeenWork,
    active: Option<SeenBatch>,
}

struct SeenBatch {
    ids: Vec<Id>,
    all_dms: bool,
    task: JoinHandle<SeenWriteErrors>,
}

impl SeenWriter {
    pub(crate) fn enqueue(&mut self, command: SeenCommand, db: &'static Storage) {
        self.collect(command);
        self.start_if_idle(db);
    }

    pub(crate) fn collect(&mut self, command: SeenCommand) {
        self.pending.add(command);
    }

    fn start_if_idle(&mut self, db: &'static Storage) {
        if self.is_writing() || self.pending.is_empty() {
            return;
        }
        let (ids, all_dms) = self.pending.take();
        let write_ids = ids.clone();
        self.active = Some(SeenBatch {
            ids,
            all_dms,
            task: task::spawn_blocking(move || write_seen_batch(db, &write_ids, all_dms)),
        });
    }

    fn is_writing(&self) -> bool {
        self.active.is_some()
    }

    /// Publish one completion and start the next batch. Safe to poll in `select!`.
    pub(crate) async fn settle_next(&mut self) {
        let result = self.next_result().await;
        for (context, error) in [
            ("Failed to save read state", &result.ids_error),
            ("Failed to mark all DMs read", &result.all_dms_error),
        ] {
            if let Some(error) = error {
                let message = format!("{context}: {error}");
                tracing::error!("{message}");
                GLOBALS.status_queue.write().write(message);
            }
        }
        let _ = GLOBALS.seen_results.send(result);
        GLOBALS.notify_ui_redraw.notify_waiters();
        self.start_if_idle(GLOBALS.db());
    }

    /// Give accepted work a chance to settle after the inbox is closed.
    pub(crate) async fn drain(&mut self) {
        let drain = async {
            self.start_if_idle(GLOBALS.db());
            while self.is_writing() {
                self.settle_next().await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .is_err()
        {
            tracing::warn!("Timed out draining seen writes; queued work may be lost and an in-flight write may still run");
        }
    }

    /// Wait for settlement without consuming the handle if another select arm wins.
    async fn next_result(&mut self) -> SeenWriteResult {
        let Some(batch) = self.active.as_mut() else {
            return std::future::pending().await;
        };
        let joined = (&mut batch.task).await;
        let SeenBatch { ids, all_dms, .. } = self.active.take().expect("joined an active batch");
        let errors = joined.unwrap_or_else(|error| {
            let error = format!("seen write task failed: {error}");
            SeenWriteErrors {
                ids_error: (!ids.is_empty()).then(|| error.clone()),
                all_dms_error: all_dms.then_some(error),
            }
        });
        SeenWriteResult {
            ids,
            all_dms,
            ids_error: errors.ids_error,
            all_dms_error: errors.all_dms_error,
        }
    }
}

/// Persist one seen-write batch synchronously.
///
/// Callers run this on `spawn_blocking` so the Overlord's async loop and the UI
/// never wait for LMDB. The mark-all-DMs path keeps using the existing scan and
/// write implementation.
fn write_seen_batch(db: &Storage, ids: &[Id], all_dms: bool) -> SeenWriteErrors {
    write_seen_batch_with(
        ids,
        all_dms,
        || db.mark_all_dms_read(),
        |ids| write_event_ids(db, ids),
    )
}

fn write_event_ids(db: &Storage, ids: &[Id]) -> Result<(), Error> {
    let mut txn = db.get_write_txn()?;
    for id in ids {
        db.mark_event_viewed(*id, Some(&mut txn))?;
    }
    txn.commit()?;
    Ok(())
}

/// Split one batch into independent operation results.
///
/// The two writes are attempted independently: a failed mark-all-DMs scan must
/// not turn a successful explicit-ID transaction into a failure, and vice
/// versa. Taking the operations as closures keeps that settlement behavior
/// testable without provoking a real LMDB failure.
fn write_seen_batch_with<A, I>(
    ids: &[Id],
    all_dms: bool,
    all_dms_write: A,
    ids_write: I,
) -> SeenWriteErrors
where
    A: FnOnce() -> Result<(), Error>,
    I: FnOnce(&[Id]) -> Result<(), Error>,
{
    let all_dms_error = if all_dms {
        all_dms_write().err().map(|error| error.to_string())
    } else {
        None
    };

    let ids_error = if ids.is_empty() {
        None
    } else {
        ids_write(ids).err().map(|error| error.to_string())
    };

    SeenWriteErrors {
        ids_error,
        all_dms_error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use std::collections::HashSet;
    use std::sync::OnceLock;

    // Storage's table handles are process-global. Use one isolated environment
    // for both these tests and the Overlord persistence tests.
    fn test_storage() -> &'static Storage {
        static DIR: OnceLock<tempdir::TempDir> = OnceLock::new();
        let dir = DIR.get_or_init(|| tempdir::TempDir::new("gossip-db-writes").unwrap());
        crate::GLOBALS.storage.get_or_init(|| {
            let storage = Storage::new(dir.path(), true).unwrap();
            // Initialize lazy tables before passing explicit write transactions.
            storage.db_event_viewed().unwrap();
            storage
        })
    }

    fn id(seed: u8) -> Id {
        Id([seed; 32])
    }

    /// Repeated commands coalesce into one deduplicated batch.
    #[test]
    fn seen_work_deduplicates_and_coalesces() {
        let mut work = SeenWork::default();
        assert!(work.is_empty());

        work.add(SeenCommand::Event(id(1)));
        work.add(SeenCommand::Event(id(1)));
        work.add(SeenCommand::Events(vec![id(1), id(2), id(2), id(3)]));
        work.add(SeenCommand::AllDms);
        work.add(SeenCommand::AllDms);
        assert!(!work.is_empty());

        let (ids, all_dms) = work.take();
        assert!(all_dms);
        let ids: HashSet<Id> = ids.into_iter().collect();
        assert_eq!(ids, HashSet::from([id(1), id(2), id(3)]));
        assert!(work.is_empty());

        // A later request is a new batch, not silently lost.
        work.add(SeenCommand::AllDms);
        assert_eq!(work.take(), (Vec::new(), true));
    }

    /// A feed "mark all read" batch writes exactly the captured targets.
    #[test]
    fn batch_writes_exact_targets() {
        let storage = test_storage();
        let targets: Vec<Id> = (20..40u8).map(id).collect();
        let untouched = id(10);
        assert!(!is_viewed(untouched).unwrap());

        let errors = write_seen_batch(storage, &targets, false);
        assert!(errors.is_empty());

        for event_id in &targets {
            assert!(is_viewed(*event_id).unwrap());
        }
        assert!(!is_viewed(untouched).unwrap());
    }

    #[tokio::test]
    async fn inbox_can_progress_while_a_seen_write_is_blocked() {
        use std::time::Duration;
        use tokio::sync::mpsc;
        use tokio::time::timeout;

        let db = test_storage();
        let ids = [id(64), id(65)];
        let held_writer = db.get_write_txn().unwrap();
        let mut writer = SeenWriter::default();
        writer.enqueue(SeenCommand::Event(ids[0]), db);

        let (tx, mut inbox) = mpsc::unbounded_channel();
        tx.send(SeenCommand::Event(ids[1])).unwrap();
        tokio::select! {
            command = inbox.recv() => writer.enqueue(command.unwrap(), db),
            result = writer.next_result() => panic!("write settled while LMDB was held: {result:?}"),
        }
        assert!(!db.is_event_viewed(ids[0]).unwrap());
        drop(held_writer);

        for id in ids {
            let result = timeout(Duration::from_secs(5), writer.next_result())
                .await
                .unwrap();
            assert_eq!(result.ids, [id]);
            assert!(result.ids_error.is_none());
            assert!(db.is_event_viewed(id).unwrap());
            writer.start_if_idle(db);
        }
        assert!(!writer.is_writing());
    }

    /// Settlement keeps a successful explicit-ID transaction separate from a
    /// failed DM scan.
    #[test]
    fn partial_dm_failure_does_not_fail_ids() {
        let ids = [id(80)];
        let errors = write_seen_batch_with(
            &ids,
            true,
            || Err(Error::from("dm scan failed")),
            |_| Ok(()),
        );

        assert!(errors.ids_error.is_none());
        assert!(errors.all_dms_error.is_some());
    }

    /// Settlement keeps a failed explicit-ID transaction separate from a
    /// successful DM scan.
    #[test]
    fn partial_id_failure_does_not_fail_all_dms() {
        let ids = [id(81)];
        let errors = write_seen_batch_with(
            &ids,
            true,
            || Ok(()),
            |_| Err(Error::from("event write failed")),
        );

        assert!(errors.ids_error.is_some());
        assert!(errors.all_dms_error.is_none());
    }

    /// Both independent operations can fail without hiding each other.
    #[test]
    fn both_operations_can_fail_independently() {
        let ids = [id(82)];
        let errors = write_seen_batch_with(
            &ids,
            true,
            || Err(Error::from("dm scan failed")),
            |_| Err(Error::from("event write failed")),
        );

        assert!(errors.ids_error.is_some());
        assert!(errors.all_dms_error.is_some());
    }

    /// Untouched operations do not acquire an error from the other operation.
    #[test]
    fn unrequested_operations_stay_clean() {
        let ids = [id(83)];
        let id_only_failure =
            write_seen_batch_with(&ids, false, || Ok(()), |_| Err(Error::from("id failure")));
        assert!(id_only_failure.ids_error.is_some());
        assert!(id_only_failure.all_dms_error.is_none());

        let dms_only_failure =
            write_seen_batch_with(&[], true, || Err(Error::from("dm failure")), |_| Ok(()));
        assert!(dms_only_failure.ids_error.is_none());
        assert!(dms_only_failure.all_dms_error.is_some());
    }
}
