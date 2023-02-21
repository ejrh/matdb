use std::collections::HashMap;
use std::rc::Rc;

use log::{debug, info};

use crate::{BlockKey, Datum, Error, SegmentNum, TransactionId};
use crate::block::Block;
use crate::database::Database;
use crate::scan::Scan;
use crate::segment::Segment;

pub struct Transaction<'db> {
    pub(crate) id: Option<TransactionId>,
    pub(crate) horizon: TransactionId,
    pub(crate) database: &'db mut Database,
    pub(crate) unsaved_blocks: HashMap<BlockKey, Rc<Block>>,
    pub(crate) uncommitted_segments: Vec<Rc<Segment>>
}

impl<'db> Transaction<'db> {
    pub fn new(database: &'db mut Database, horizon: TransactionId) -> Transaction {
        Transaction {
            id: None,
            horizon,
            database,
            unsaved_blocks: Default::default(),
            uncommitted_segments: Vec::new()
        }
    }

    pub fn add_row(&mut self, values: &[Datum]) {
        let key = self.database.schema.get_chunk_key(values);
        let block = self.unsaved_blocks.entry(key)
            .or_insert_with(|| Rc::new(Block::new(self.database.schema.dimensions.len())));
        let block = block.as_ref();
        let block = unsafe {
            let const_ptr = block as *const Block;
            let mut_ptr = const_ptr as *mut Block;
            &mut *mut_ptr
        };
        block.add_row(values);
    }

    /**
     * Discard all changes from this transaction, and clean up any data files created
     * as part of it.
     *
     * Consumes the Transaction, because you can't use it for anything else after this.
     */
    pub fn rollback(mut self) {
        self.unsaved_blocks.clear();
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
        let source = self.database.get_scan_source();
        let num_dims = self.database.schema.dimensions.len();
        let mut scan = Scan::new(source, num_dims, self.id.unwrap_or(0));
        for seg_id in self.database.get_visible_committed_segments(self.horizon) {
            debug!("Add committed segment {:?}", seg_id);
            scan.add_segment_id(seg_id);
        }
        for rc in &self.uncommitted_segments {
            debug!("Add uncommitted segment {:?}", rc.id);
            scan.add_segment(rc.clone());
        }

        for block in self.unsaved_blocks.values() {
            debug!("Add unsaved block");
            scan.add_block(block.clone());
        }
        scan
    }

    /**
     * Create a new segment and save all remaining blocks to into.
     */
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.unsaved_blocks.is_empty() { return Ok(()); }

        let txn_id= self.get_transaction_id();
        let seg_num = self.uncommitted_segments.len() as SegmentNum;

        /* Create a new segment and save all remaining blocks to into. */
        let moved_blocks = std::mem::take(&mut self.unsaved_blocks);
        let blocks: Vec<&Rc<Block>> = moved_blocks.values().collect();
        let mut block_refs = Vec::new();
        for rc in blocks {
            let br = unsafe {
                let x = rc.as_ref() as *const Block;
                let y = x as *mut Block;
                &*y
            };
            block_refs.push(br);
        }

        let seg_id = (txn_id, seg_num);
        let new_segment = Segment::create(
            self.database.path.as_path(),
            seg_id, &block_refs
        )?;

        let rc = Rc::new(new_segment);
        self.uncommitted_segments.push(rc);
        //TODO tell database to cache the segment for us
        Ok(())
    }

    /**
     * Rename the segment files so they're visible to other transactions.
     *
     * We do this in reverse order: the database won't see the transaction
     * until segment 1 is visible.
     */
    fn commit_segments(&mut self) -> Result<(), Error>{
        while !self.uncommitted_segments.is_empty() {
            let mut rc = self.uncommitted_segments.pop().unwrap();
            let segment = Rc::get_mut(&mut rc).unwrap();
            segment.make_visible(&self.database.path)?;
            self.database.add_committed_segment(segment.id);
            debug!("Made segment visible {:?}", segment.path);
        }
        Ok(())
    }

    /**
     * Delete any temporary segment files.
     */
    fn rollback_segments(&mut self) {
        let moved_segments = std::mem::take(&mut self.uncommitted_segments);
        for mut rc in moved_segments {
            let segment = Rc::get_mut(&mut rc).unwrap();
            let path = segment.path.clone();
            segment.delete().unwrap();
            debug!("Deleted cancelled segment {:?}", path);
            //TODO tell database to stop caching the segment
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
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        self.unsaved_blocks.clear();
        self.rollback_segments();
    }
}
