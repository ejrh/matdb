use std::path::{Path, PathBuf};

use log::{debug, info};

use crate::{Error, SegmentId, TransactionId};
use crate::cache::Cache;
use crate::schema::Schema;
use crate::segment::Segment;
use crate::transaction::Transaction;

use crate::storage::{decode_segment_path};

pub struct Database {
    pub path: PathBuf,
    pub schema: Schema,
    pub next_transaction_id: TransactionId,
    pub cached_segments: Cache<(TransactionId, SegmentId), Segment>
}

struct ScanResult {
    pub next_transaction_id: TransactionId
}

impl Database {
    pub fn create(schema: Schema, path: &Path) -> Result<Database, Error> {
        std::fs::create_dir(path)?;
        schema.save(path)?;
        info!("Created database in {:?}", path);
        debug!("Dimensions: {:?}", schema.dimensions.iter().map(|d| (&d.name, d.chunk_size)).collect::<Vec<_>>());
        debug!("Values: {:?}", schema.values.iter().map(|v| &v.name).collect::<Vec<_>>());

        Ok(Database {
            path: path.to_path_buf(),
            schema,
            next_transaction_id: 1,
            cached_segments: Cache::new(),
        })
    }

    pub fn open(path: &Path) -> Result<Database, Error> {
        let schema = Schema::load(path)?;
        let scan = scan_files(path)?;
        info!("Opened database in {:?}", path);
        Ok(Database {
            path: path.to_path_buf(),
            schema,
            next_transaction_id: scan.next_transaction_id,
            cached_segments: Cache::new(),
        })
    }

    pub fn new_transaction<'db>(&'db mut self) -> Result<Transaction<'db>, Error> {
        info!("Created transaction with horizon < {:?}", self.next_transaction_id);
        Ok(Transaction::new(self))
    }

    pub(crate) fn get_next_transaction_id(&mut self) -> TransactionId {
        let txn_id = self.next_transaction_id;
        self.next_transaction_id += 1;
        info!("Allocated transaction id {:?}", txn_id);
        txn_id
    }
}

fn scan_files(database_path: &Path) -> Result<ScanResult, Error> {
    let mut max_seen_id = 0;
    for entry in std::fs::read_dir(database_path)? {
        let entry = entry.unwrap();
        if let Some((txn_id, _, _)) = decode_segment_path(&entry.path()) {
            if txn_id > max_seen_id {
                max_seen_id = txn_id;
            }
        };
    }

    Ok(ScanResult {
        next_transaction_id: max_seen_id + 1,
    })
}
