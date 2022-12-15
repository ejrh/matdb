use std::collections::HashMap;

use log::{debug, info};

use crate::{BlockKey, Datum, Error, SegmentId, TransactionId};
use crate::block::Block;
use crate::database::Database;
use crate::scan::Scan;
use crate::segment::Segment;

pub struct Transaction<'db> {
    pub(crate) id: Option<TransactionId>,
    pub(crate) database: &'db mut Database,
    pub(crate) blocks: HashMap<BlockKey, Block>,
    pub(crate) segments: Vec<Segment>
}

impl<'db> Transaction<'db> {
    pub fn new(database: &'db mut Database) -> Transaction {
        Transaction {
            id: None,
            database,
            blocks: Default::default(),
            segments: Vec::new()
        }
    }

    pub fn add_row(&mut self, values: &[Datum]) {
        let key = self.database.schema.get_chunk_key(values);
        let block = self.blocks.entry(key).or_insert_with(|| Block::new(self.database.schema.dimensions.len()));
        block.add_row(values);
    }

    /**
     * Discard all changes from this transaction, and clean up any data files created
     * as part of it.
     *
     * Consumes the Transaction, because you can't use it for anything else after this.
     */
    pub fn rollback(mut self) {
        self.blocks.clear();
        self.rollback_segments();
    }

    /**
     * Save all changes from this transaction, making them visible for future transactions.
     *
     * Consumes the Transaction, because you can't use it for anything else after this.
     */
    pub fn commit(mut self) -> Result<(), Error> {
        self.flush()?;
        self.commit_segments()?;
        info!("Committed transaction with id {:?}", self.id);
        Ok(())
    }

    pub fn query(&'db self) -> Scan<'db> {
        Scan::new(self.id.unwrap_or(0))
    }

    /**
     * Create a new segment and save all remaining blocks to into.
     */
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.blocks.is_empty() { return Ok(()); }

        let txn_id= self.get_transaction_id();
        let seg_id = self.segments.len() as SegmentId;

        /* Create a new segment and save all remaining blocks to into. */
        let moved_blocks = std::mem::take(&mut self.blocks);

        let new_segment = Segment::create(
            self.database.path.as_path(),
            txn_id, seg_id, moved_blocks
        )?;

        self.segments.push(new_segment);
        Ok(())
    }

    /**
     * Rename the segment files so they're visible to other transactions.
     *
     * We do this in reverse order: the database won't see the transaction
     * until segment 1 is visible.
     */
    fn commit_segments(&mut self) -> Result<(), Error>{
        while !self.segments.is_empty() {
            let segment = self.segments.pop();
            let mut segment = segment.unwrap();
            segment.make_visible(&self.database.path)?;
            debug!("Made segment visible {:?}", segment.path);
        }
        Ok(())
    }

    /**
     * Delete any temporary segment files.
     */
    fn rollback_segments(&mut self) {
        let moved_segments = std::mem::take(&mut self.segments);
        for segment in moved_segments {
            let path = segment.path.clone();
            segment.delete().unwrap();
            debug!("Deleted cancelled segment {:?}", path);
        }
    }

    fn get_transaction_id(&mut self) -> TransactionId {
        if self.id.is_some() {
            self.id.unwrap()
        } else {
            let id = self.database.get_next_transaction_id();
            self.id = Some(id);
            id
        }
    }

    pub(crate) fn get_visible_segments(&self) -> Vec<(TransactionId, SegmentId)> {
        let segments = self.database.get_committed_segments();
        //segments.extend(self.segments.iter().map(|s|))
        segments
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        self.blocks.clear();
        self.rollback_segments();
    }
}
