use std::path::{Path, PathBuf};
use crate::{Error, TransactionId};
use crate::schema::Schema;
use crate::transaction::Transaction;

use crate::storage::{decode_segment_path};

pub struct Database {
    pub path: PathBuf,
    pub schema: Schema,
    pub next_transaction_id: TransactionId
}

struct ScanResult {
    pub next_transaction_id: TransactionId
}

impl Database {
    pub fn create(schema: Schema, path: &Path) -> Result<Database, Error> {
        std::fs::create_dir(path)?;
        schema.save(path)?;
        Ok(Database {
            path: path.to_path_buf(),
            schema,
            next_transaction_id: 1
        })
    }

    pub fn open(path: &Path) -> Result<Database, Error> {
        let schema = Schema::load(path)?;
        let scan = scan_files(path)?;
        Ok(Database {
            path: path.to_path_buf(),
            schema,
            next_transaction_id: scan.next_transaction_id
        })
    }

    pub fn new_transaction<'db>(&'db mut self) -> Result<Transaction<'db>, Error> {
        Ok(Transaction::new(self))
    }

    pub(crate) fn get_next_transaction_id(&mut self) -> TransactionId {
        let txn_id = self.next_transaction_id;
        self.next_transaction_id += 1;
        txn_id
    }
}

fn scan_files(database_path: &Path) -> Result<ScanResult, Error> {
    let mut max_seen_id = 0;
    for entry in std::fs::read_dir(database_path)? {
        let entry = entry.unwrap();
        match decode_segment_path(&entry.path()) {
            Some((txn_id, _, _)) => {
                if txn_id > max_seen_id {
                    max_seen_id = txn_id;
                }
            },
            None => ()
        }
    }

    Ok(ScanResult {
        next_transaction_id: max_seen_id + 1,
    })
}
