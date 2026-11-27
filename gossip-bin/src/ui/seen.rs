//! Optimistic seen (viewed) state owned by the UI.
//!
//! The Overlord persists viewed ids in batches. While a batch is in flight the
//! UI paints its ids as viewed and avoids submitting them again. Failed ids
//! stay unread and are not retried automatically on hover, but an explicit
//! bulk action (mark all read) may retry them.

use gossip_lib::{comms::ToOverlordMessage, SeenCommand, SeenWriteResult, GLOBALS};
use nostr_types::Id;
use std::collections::HashSet;
use tokio::sync::broadcast::{error::TryRecvError, Receiver};

pub(super) struct Seen {
    state: PendingReads,
    results: Receiver<SeenWriteResult>,
    dm_counts_dirty: bool,
}

impl Seen {
    pub(super) fn new() -> Self {
        Self {
            state: PendingReads::default(),
            results: GLOBALS.seen_results.subscribe(),
            dm_counts_dirty: false,
        }
    }

    pub(super) fn is_viewed(&self, id: Id) -> bool {
        self.state.is_pending(id) || gossip_lib::seen::is_viewed(id).unwrap_or(false)
    }

    pub(super) fn mark_hovered(&mut self, id: Id) {
        if self.state.is_pending(id) || self.state.is_failed(id) {
            return;
        }
        if Self::submit(SeenCommand::Event(id)) {
            self.state.mark_pending([id]);
        } else {
            self.state.mark_failed(&[id]);
        }
    }

    pub(super) fn mark_many_read(&mut self, ids: Vec<Id>) {
        let ids = self.state.filter_submittable(ids);
        if ids.is_empty() {
            return;
        }
        if Self::submit(SeenCommand::Events(ids.clone())) {
            self.state.mark_pending(ids);
        } else {
            self.state.mark_failed(&ids);
        }
    }

    pub(super) fn mark_all_dms_read(&mut self) {
        if !self.state.all_dms_pending() && Self::submit(SeenCommand::AllDms) {
            self.state.mark_all_dms_pending();
        }
    }

    fn submit(command: SeenCommand) -> bool {
        GLOBALS
            .to_overlord
            .send(ToOverlordMessage::Seen(command))
            .is_ok()
    }

    pub(super) fn update(&mut self) {
        loop {
            match self.results.try_recv() {
                Ok(result) => {
                    self.state.reconcile(&result);
                    self.dm_counts_dirty |= result.all_dms && result.all_dms_error.is_none();
                }
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                Err(TryRecvError::Lagged(_)) => self.state.clear_after_lag(),
            }
        }
    }

    pub(super) fn take_dm_counts_dirty(&mut self) -> bool {
        std::mem::take(&mut self.dm_counts_dirty)
    }
}

#[derive(Debug, Default)]
struct PendingReads {
    pending: HashSet<Id>,
    failed: HashSet<Id>,
    all_dms_pending: bool,
}

impl PendingReads {
    fn is_pending(&self, id: Id) -> bool {
        self.pending.contains(&id)
    }

    fn is_failed(&self, id: Id) -> bool {
        self.failed.contains(&id)
    }

    fn all_dms_pending(&self) -> bool {
        self.all_dms_pending
    }

    /// Remove ids that are already being persisted and duplicate ids within the
    /// submitted list.
    ///
    /// Failed ids are intentionally kept: a bulk mark-all-read is an explicit
    /// retry. The returned list is only submitted when non-empty.
    fn filter_submittable(&self, ids: Vec<Id>) -> Vec<Id> {
        let mut seen = HashSet::with_capacity(ids.len());
        ids.into_iter()
            .filter(|id| !self.pending.contains(id) && seen.insert(*id))
            .collect()
    }

    /// Record ids accepted by the Overlord. An explicit submission clears a
    /// previous failure for the same id.
    fn mark_pending(&mut self, ids: impl IntoIterator<Item = Id>) {
        for id in ids {
            self.failed.remove(&id);
            self.pending.insert(id);
        }
    }

    /// Record ids that could not even be handed to the Overlord.
    fn mark_failed(&mut self, ids: &[Id]) {
        for id in ids {
            self.pending.remove(id);
            self.failed.insert(*id);
        }
    }

    fn mark_all_dms_pending(&mut self) {
        self.all_dms_pending = true;
    }

    /// Reconcile one settled batch. The explicit-ID transaction and the
    /// mark-all-DMs scan settle independently.
    fn reconcile(&mut self, result: &SeenWriteResult) {
        let ids_failed = result.ids_error.is_some();
        for id in &result.ids {
            self.pending.remove(id);
            if ids_failed {
                self.failed.insert(*id);
            } else {
                self.failed.remove(id);
            }
        }

        if result.all_dms {
            // Either outcome ends the optimistic DM pending state. A failure is
            // reported by the Overlord; the button may be clicked again.
            self.all_dms_pending = false;
        }
    }

    /// Settlement messages were dropped. Fall back to persisted state instead
    /// of keeping potentially-wrong optimistic ids.
    fn clear_after_lag(&mut self) {
        self.pending.clear();
        self.all_dms_pending = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(seed: u8) -> Id {
        Id([seed; 32])
    }

    fn result(
        ids: Vec<Id>,
        ids_error: Option<&str>,
        all_dms: bool,
        all_dms_error: Option<&str>,
    ) -> SeenWriteResult {
        SeenWriteResult {
            ids,
            ids_error: ids_error.map(str::to_owned),
            all_dms,
            all_dms_error: all_dms_error.map(str::to_owned),
        }
    }

    #[test]
    fn filters_pending_and_duplicates_but_keeps_failed_retry() {
        let mut state = PendingReads::default();
        state.mark_pending([id(1)]);
        state.mark_failed(&[id(2)]);

        let submit = state.filter_submittable(vec![id(1), id(1), id(2), id(2), id(3), id(3)]);
        assert_eq!(submit, vec![id(2), id(3)]);

        state.mark_pending(submit.iter().copied());
        assert!(state.is_pending(id(2)));
        assert!(!state.is_failed(id(2)));
        assert!(state.is_pending(id(3)));

        // Nothing pending is submitted twice.
        assert!(state
            .filter_submittable(vec![id(1), id(2), id(3)])
            .is_empty());
    }

    #[test]
    fn dm_failure_does_not_fail_id_success() {
        let mut state = PendingReads::default();
        state.mark_pending([id(4)]);
        state.mark_all_dms_pending();

        state.reconcile(&result(vec![id(4)], None, true, Some("dm scan failed")));

        assert!(!state.is_pending(id(4)));
        assert!(!state.is_failed(id(4)));
        assert!(!state.all_dms_pending());
    }

    #[test]
    fn id_failure_does_not_hide_dm_success() {
        let mut state = PendingReads::default();
        state.mark_pending([id(5)]);
        state.mark_all_dms_pending();

        state.reconcile(&result(vec![id(5)], Some("event write failed"), true, None));

        assert!(!state.is_pending(id(5)));
        assert!(state.is_failed(id(5)));
        assert!(!state.all_dms_pending());
    }

    #[test]
    fn explicit_retry_clears_failed_id() {
        let mut state = PendingReads::default();
        state.mark_failed(&[id(6)]);
        let submit = state.filter_submittable(vec![id(6)]);
        assert_eq!(submit, vec![id(6)]);

        state.mark_pending(submit);
        assert!(state.is_pending(id(6)));
        assert!(!state.is_failed(id(6)));
    }

    #[test]
    fn lag_falls_back_to_persisted_state() {
        let mut state = PendingReads::default();
        state.mark_pending([id(7)]);
        state.mark_all_dms_pending();

        state.clear_after_lag();

        assert!(state.filter_submittable(vec![id(7)]) == vec![id(7)]);
        assert!(!state.all_dms_pending());
    }
}
