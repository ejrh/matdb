use std::collections::{HashSet};
use std::path::{Path, PathBuf};

use log::{debug, info};

use crate::{Error, SegmentId, TransactionId};
use crate::cache::Cache;
use crate::schema::Schema;
use crate::segment::Segment;
use crate::storage::decode_segment_path;
use crate::transaction::Transaction;

pub struct Database {
    pub path: PathBuf,
    pub schema: Schema,
    pub next_transaction_id: TransactionId,
    pub committed_segments: HashSet<SegmentId>,
    pub cached_segments: Cache<SegmentId, Segment>
}

struct ScanResult {
    next_transaction_id: TransactionId,
    committed_segments: HashSet<SegmentId>
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
            committed_segments: HashSet::new(),
            cached_segments: Cache::new(),
        })
    }

    pub fn open(path: &Path) -> Result<Database, Error> {
        let schema = Schema::load(path)?;
        let scan = scan_files(path)?;
        info!("Opened database in {:?}", path);
        debug!("Next transaction is {:?}, number of committed segments is {:?}",
            scan.next_transaction_id, scan.committed_segments.len());
        Ok(Database {
            path: path.to_path_buf(),
            schema,
            next_transaction_id: scan.next_transaction_id,
            committed_segments: scan.committed_segments,
            cached_segments: Cache::new(),
        })
    }

    pub fn new_transaction(&mut self) -> Result<Transaction, Error> {
        let horizon = self.next_transaction_id;
        info!("Created transaction with horizon < {:?}", horizon);
        Ok(Transaction::new(self, horizon))
    }

    pub(crate) fn get_next_transaction_id(&mut self) -> TransactionId {
        let txn_id = self.next_transaction_id;
        self.next_transaction_id += 1;
        info!("Allocated transaction id {:?}", txn_id);
        txn_id
    }

    pub(crate) fn add_committed_segment(&mut self, seg_id: SegmentId) {
        self.committed_segments.insert(seg_id);
    }

    pub(crate) fn get_visible_committed_segments(&self, horizon: TransactionId) -> Vec<SegmentId> {
        let mut segments = Vec::new();
        segments.extend(self.committed_segments.iter().filter(|&seg| seg.0 < horizon));
        segments
    }
}

fn scan_files(database_path: &Path) -> Result<ScanResult, Error> {
    let mut max_seen_txn_id = 0;
    let mut known_segments = HashSet::new();
    for entry in std::fs::read_dir(database_path)? {
        let entry = entry.unwrap();
        if let Some((txn_id, seg_num, _)) = decode_segment_path(&entry.path()) {
            if txn_id > max_seen_txn_id {
                max_seen_txn_id = txn_id;
            }

            let seg_id = (txn_id, seg_num);
            known_segments.insert(seg_id);
        };
    }

    //TODO any transaction with no segment 0 didn't commit fully, so ignore those segments

    Ok(ScanResult {
        next_transaction_id: max_seen_txn_id + 1,
        committed_segments: known_segments
    })
}
