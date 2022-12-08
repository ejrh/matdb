use std::collections::HashMap;

use crate::{BlockKey, Datum, Error, QueryIterator, SegmentId, TransactionId};
use crate::block::Block;
use crate::database::Database;
use crate::segment::Segment;

pub struct Transaction<'db> {
    id: Option<TransactionId>,
    database: &'db mut Database,
    blocks: HashMap<BlockKey, Block>,
    segments: Vec<Segment>
}

impl<'db> Transaction<'db> {
    pub fn new(database: &'db mut Database) -> Transaction {
        Transaction {
            id: None,
            database: database,
            blocks: Default::default(),
            segments: Vec::new()
        }
    }

    pub fn add_row(&mut self, values: &[Datum]) -> Result<(), Error> {
        let key = self.database.schema.get_chunk_key(values);
        let ref mut block = self.blocks.entry(key).or_insert_with(|| Block::new(self.database.schema.dimensions.len()));
        block.add_row(values);
        Ok(())
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
        Ok(())
    }

    pub fn query(&'db self, values_array: &mut Vec<Datum>) -> QueryIterator<'db> {
        QueryIterator {
            block_iter: self.blocks.iter(),
            block_key: None,
            value_iter: None,
            values_array
        }
    }

    /**
     * Create a new segment and save all remaining blocks to into.
     */
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.blocks.is_empty() { return Ok(()); }

        let txn_id= self.get_transaction_id();
        let seg_id = self.segments.len() as SegmentId;

        /* Create a new segment and save all remaining blocks to into. */
        let moved_blocks = std::mem::replace(&mut self.blocks, Default::default());

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
        }
        Ok(())
    }

    /**
     * Delete any temporary segment files.
     */
    fn rollback_segments(&mut self) {
        let moved_segments = std::mem::replace(&mut self.segments, Vec::new());
        for segment in moved_segments {
            segment.delete().unwrap();
        }
    }

    fn get_transaction_id(&mut self) -> TransactionId {
        if self.id.is_some() {
            return self.id.unwrap();
        } else {
            let id = self.database.get_next_transaction_id();
            self.id = Some(id);
            id
        }
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        self.blocks.clear();
        self.rollback_segments();
    }
}
