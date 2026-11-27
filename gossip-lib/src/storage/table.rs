use super::types::{ByteRep, Record};
use crate::error::{Error, ErrorKind};
use crate::globals::GLOBALS;
use heed::types::Bytes;
use heed::{Database, RoTxn, RwTxn};

pub trait Table {
    type Item: Record;

    fn lmdb_name() -> &'static str;

    /// Get the heed database
    fn db() -> Result<Database<Bytes, Bytes>, Error>;

    /// Number of records
    #[allow(dead_code)]
    fn num_records() -> Result<u64, Error> {
        let txn = GLOBALS.db().env.read_txn()?;
        Ok(Self::db()?.len(&txn)?)
    }

    /// Bytes used
    #[allow(dead_code)]
    fn bytes_used() -> Result<usize, Error> {
        let txn = GLOBALS.db().env.read_txn()?;
        let stat = Self::db()?.stat(&txn)?;
        Ok(stat.page_size as usize
            * (stat.branch_pages + stat.leaf_pages + stat.overflow_pages + 2))
    }

    /// Write a record
    /// (it needs to be mutable for possible stabilization)
    #[allow(dead_code)]
    fn write_record(record: &mut Self::Item, rw_txn: Option<&mut RwTxn<'_>>) -> Result<(), Error> {
        record.stabilize();
        let keybytes = record.key().to_bytes()?;
        let valbytes = record.to_bytes()?;

        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        Self::db()?.put(txn, &keybytes, &valbytes)?;

        maybe_local_txn_commit!(local_txn);

        Ok(())
    }

    /// Write a default record if missing
    #[allow(dead_code)]
    fn create_record_if_missing(
        key: <Self::Item as Record>::Key,
        rw_txn: Option<&mut RwTxn<'_>>,
    ) -> Result<(), Error> {
        let keybytes = key.to_bytes()?;

        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        if Self::db()?.get(txn, &keybytes)?.is_none() {
            let mut record = match <Self::Item as Record>::new(key) {
                Some(r) => r,
                None => return Err(ErrorKind::RecordIsNotNewable.into()),
            };
            record.stabilize();
            let valbytes = record.to_bytes()?;
            Self::db()?.put(txn, &keybytes, &valbytes)?;
        }

        maybe_local_txn_commit!(local_txn);

        Ok(())
    }

    /// Check if a record exists
    #[allow(dead_code)]
    fn has_record(
        key: <Self::Item as Record>::Key,
        rw_txn: Option<&RoTxn<'_>>,
    ) -> Result<bool, Error> {
        let keybytes = key.to_bytes()?;

        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        let rval = Self::db()?.get(txn, &keybytes)?.is_some();

        maybe_local_txn_commit!(local_txn);

        Ok(rval)
    }

    /// Read a record
    #[allow(dead_code)]
    fn read_record(
        key: <Self::Item as Record>::Key,
        rtxn: Option<&RoTxn<'_>>,
    ) -> Result<Option<Self::Item>, Error> {
        let keybytes = key.to_bytes()?;
        // Lazy table initialization may require a write transaction.
        let db = Self::db()?;
        let local_txn;
        let txn = match rtxn {
            Some(txn) => txn,
            None => {
                local_txn = GLOBALS.db().get_read_txn()?;
                &local_txn
            }
        };

        db.get(txn, &keybytes)?
            .map(Self::Item::from_bytes)
            .transpose()
    }

    /// Read a record or create a new one (and store it)
    ///
    /// Will error if the Record is not newable
    #[allow(dead_code)]
    fn read_or_create_record(
        key: <Self::Item as Record>::Key,
        rw_txn: Option<&mut RwTxn<'_>>,
    ) -> Result<Self::Item, Error> {
        let keybytes = key.to_bytes()?;

        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        let rval = {
            let valbytes = Self::db()?.get(txn, &keybytes)?;
            match valbytes {
                None => {
                    let mut record = match <Self::Item as Record>::new(key) {
                        Some(r) => r,
                        None => return Err(ErrorKind::RecordIsNotNewable.into()),
                    };
                    record.stabilize();
                    let valbytes = record.to_bytes()?;
                    Self::db()?.put(txn, &keybytes, &valbytes)?;
                    record
                }
                Some(valbytes) => <Self::Item>::from_bytes(valbytes)?,
            }
        };

        maybe_local_txn_commit!(local_txn);

        Ok(rval)
    }

    /// delete_record
    fn delete_record(
        key: <Self::Item as Record>::Key,
        rw_txn: Option<&mut RwTxn<'_>>,
    ) -> Result<(), Error> {
        let keybytes = key.to_bytes()?;

        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        Self::db()?.delete(txn, &keybytes)?;

        maybe_local_txn_commit!(local_txn);

        Ok(())
    }

    /// filter_records
    fn filter_records<F>(f: F) -> Result<Vec<Self::Item>, Error>
    where
        F: Fn(&Self::Item) -> bool,
    {
        let txn = GLOBALS.db().get_read_txn()?;

        let iter = Self::db()?.iter(&txn)?;
        let mut output: Vec<Self::Item> = Vec::new();
        for result in iter {
            let (_keybytes, valbytes) = result?;
            let record = <Self::Item>::from_bytes(valbytes)?;
            if f(&record) {
                output.push(record);
            }
        }

        Ok(output)
    }

    /// Modify a record in the database if it exists; returns false if not found
    #[allow(dead_code)]
    fn modify_if_exists<M>(
        key: <Self::Item as Record>::Key,
        mut modify: M,
        rw_txn: Option<&mut RwTxn<'_>>,
    ) -> Result<bool, Error>
    where
        M: FnMut(&mut Self::Item),
    {
        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        let keybytes = key.to_bytes()?;
        let valbytes = Self::db()?.get(txn, &keybytes)?;
        let mut record = match valbytes {
            Some(valbytes) => Self::Item::from_bytes(valbytes)?,
            None => return Ok(false),
        };
        modify(&mut record);
        record.stabilize();
        let valbytes = record.to_bytes()?;
        Self::db()?.put(txn, &keybytes, &valbytes)?;

        maybe_local_txn_commit!(local_txn);

        Ok(true)
    }

    /// Modify a record in the database; create first if missing
    ///
    /// Will error if the Record is not newable (see modify_if_exists)
    #[allow(dead_code)]
    fn modify<M>(
        key: <Self::Item as Record>::Key,
        mut modify: M,
        rw_txn: Option<&mut RwTxn<'_>>,
    ) -> Result<(), Error>
    where
        M: FnMut(&mut Self::Item),
    {
        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        {
            let keybytes = key.to_bytes()?;
            let valbytes = Self::db()?.get(txn, &keybytes)?;
            let mut record = match valbytes {
                Some(valbytes) => Self::Item::from_bytes(valbytes)?,
                None => match Self::Item::new(key) {
                    Some(r) => r,
                    None => return Err(ErrorKind::RecordIsNotNewable.into()),
                },
            };
            modify(&mut record);
            record.stabilize();
            let valbytes = record.to_bytes()?;
            Self::db()?.put(txn, &keybytes, &valbytes)?;
        }

        maybe_local_txn_commit!(local_txn);

        Ok(())
    }

    /// Modify all matching records in the database
    #[allow(dead_code)]
    fn filter_modify<F, M>(f: F, mut modify: M, rw_txn: Option<&mut RwTxn<'_>>) -> Result<(), Error>
    where
        F: Fn(&Self::Item) -> bool,
        M: FnMut(&mut Self::Item),
    {
        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        {
            let mut iter = Self::db()?.iter_mut(txn)?;
            while let Some(result) = iter.next() {
                let (keybytes, valbytes) = result?;
                let mut record = Self::Item::from_bytes(valbytes)?;
                if f(&record) {
                    modify(&mut record);
                    record.stabilize();
                    let valbytes = record.to_bytes()?;
                    let keybytes = keybytes.to_owned();
                    unsafe {
                        iter.put_current(&keybytes, &valbytes)?;
                    }
                }
            }
        }

        maybe_local_txn_commit!(local_txn);

        Ok(())
    }

    fn iter<'a>(txn: &'a RoTxn<'a>) -> Result<TableIterator<'a, Self::Item>, Error> {
        Ok(TableIterator {
            inner: Self::db()?.iter(txn)?,
            phantom: std::marker::PhantomData,
        })
    }

    fn clear(rw_txn: Option<&mut RwTxn<'_>>) -> Result<(), Error> {
        let mut local_txn = None;
        let txn = maybe_local_txn!(GLOBALS.db(), rw_txn, local_txn);

        Self::db()?.clear(txn)?;

        maybe_local_txn_commit!(local_txn);

        Ok(())
    }
}

pub struct TableIterator<'a, I: Record> {
    inner: heed::RoIter<'a, Bytes, Bytes>,
    phantom: std::marker::PhantomData<I>,
}

impl<I: Record> Iterator for TableIterator<'_, I> {
    type Item = (I::Key, I);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(result) => {
                if let Ok((keybytes, valbytes)) = result {
                    match (I::Key::from_bytes(keybytes), I::from_bytes(valbytes)) {
                        (Ok(key), Ok(record)) => Some((key, record)),
                        _ => None,
                    }
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{test_storage, Person3Table, PersonTable};
    use nostr_types::PublicKey;
    use std::{sync::mpsc, thread, time::Duration};

    #[test]
    fn read_record_initializes_a_missing_table() {
        let storage = test_storage();
        let key = PublicKey::from_bytes(&[43; 32], false).unwrap();

        // Use the migration table so this test doesn't depend on whether the
        // writer-contention test has already opened the current person table.
        let txn = storage.get_read_txn().unwrap();
        assert!(storage
            .env
            .open_database::<Bytes, Bytes>(&txn, Some(Person3Table::lmdb_name()))
            .unwrap()
            .is_none());
        drop(txn);

        assert!(Person3Table::read_record(key, None).unwrap().is_none());
        let txn = storage.get_read_txn().unwrap();
        assert!(storage
            .env
            .open_database::<Bytes, Bytes>(&txn, Some(Person3Table::lmdb_name()))
            .unwrap()
            .is_some());
        txn.commit().unwrap();

        Person3Table::create_record_if_missing(key, None).unwrap();
        assert_eq!(
            Person3Table::read_record(key, None)
                .unwrap()
                .unwrap()
                .pubkey,
            key
        );
    }

    #[test]
    fn read_record_does_not_wait_for_writer() {
        let storage = test_storage();
        let key = PublicKey::from_bytes(&[42; 32], false).unwrap();
        // Initialize the lazy table before any operation starts a write transaction.
        PersonTable::db().unwrap();
        PersonTable::create_record_if_missing(key, None).unwrap();
        let writer = storage.get_write_txn().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = thread::spawn(move || {
            let _ = send.send(PersonTable::read_record(key, None));
        });

        let read_while_locked = receive.recv_timeout(Duration::from_secs(2));
        drop(writer);
        reader.join().unwrap();
        let person = read_while_locked
            .expect("read_record waited for the LMDB writer")
            .unwrap()
            .expect("committed person should be readable");
        assert_eq!(person.pubkey, key);
    }
}
