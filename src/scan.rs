use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;
use std::rc::Rc;
use log::{debug, error, info};

use crate::block::{Block, BlockIter};
use crate::{BlockId, compare_points, Datum, SegmentId, TransactionId};
use crate::query::QueryRow;
use crate::segment::Segment;

/**
 * Something that can provide segments and blocks to a scan.
 */
pub(crate) trait ScanSource {
    fn get_segment(&self, seg_id: SegmentId) -> Option<Rc<Segment>>;
    fn get_block(&self, block_id: BlockId) -> Option<Rc<Block>>;
}

pub(crate) enum Type {
    SegmentId(SegmentId),
    Segment(Rc<Segment>),
    BlockId(BlockId),
    Block(Rc<Block>)
}

pub(crate) struct QueuedItem {
    start_point: Vec<Datum>,
    item_type: Type
}

#[derive(Clone)]
pub(crate) struct LiveItem {
    iter: BlockIter,
    current: Option<Vec<Datum>>,
    txn_id: TransactionId
}

/**
 * A scan is an iterator that keeps track of blocks, or things that can provide blocks (like
 * segments), extracts rows from them, and merges the rows so that only the best version of each is
 * returned.
 *
 * These items can overlap.  As the block sorter progresses through all the items it maintains a
 * set of live block iterators.
 *
 * When a row is fetched from the iterator, the first row from each live iterator is checked.  The
 * best one is returned from the iterator.  Other rows are either kept for next time, or discarded.
 * Discarded rows are those with the same key as the best row, but from an older transaction.
 *
 * When the current row id reaches the next start point, any blocks containing that point are
 * dequeued and their iterator is added to the live set.
 *
 * When a block iterator is exhausted, it is removed from the live set.
 */
pub struct Scan<'txn> {
    source: Box<dyn ScanSource + 'txn>,
    num_dims: usize,
    this_txn_id: TransactionId,
    queue: BinaryHeap<QueuedItem>,
    live: Vec<LiveItem>
}

impl<'txn> Scan<'txn> {
    pub(crate) fn new(source: Box<dyn ScanSource + 'txn>, num_dims: usize, txn_id: TransactionId) -> Scan<'txn> {
        Scan {
            source,
            num_dims,
            this_txn_id: txn_id,
            queue: Default::default(),
            live: Default::default()
        }
    }

    pub(crate) fn add_segment_id(&mut self, seg_id: SegmentId) {
        let start_point = Some(vec![0, 0]);  //TODO should know the segment coords
        if start_point.is_none() {
            return;
        }
        let start_point = start_point.unwrap();
        self.queue.push(QueuedItem {
            start_point,
            item_type: Type::SegmentId(seg_id)
        })
    }

    pub(crate) fn add_segment(&mut self, segment: Rc<Segment>) {
        let start_point = Some(vec![0, 0]);  //TODO should know the segment coords
        if start_point.is_none() {
            return;
        }
        let start_point = start_point.unwrap();
        self.queue.push(QueuedItem {
            start_point,
            item_type: Type::Segment(segment)
        });
    }

    pub(crate) fn add_block_id(&mut self, block_id: BlockId) {
        let start_point = Some(vec![0, 0]);  //TODO should know the segment coords
        if start_point.is_none() {
            return;
        }
        let start_point = start_point.unwrap();
        self.queue.push(QueuedItem {
            start_point,
            item_type: Type::BlockId(block_id)
        })
    }

    pub(crate) fn add_block(&mut self, block: Rc<Block>) {
        let start_point = block.get_start_point();
        if start_point.is_none() {
            info!("Not enqueuing empty block");
            return;
        }
        let start_point = start_point.unwrap();
        debug!("Enqueued block starting at {:?}", start_point);
        self.queue.push(QueuedItem {
            start_point,
            item_type: Type::Block(block)
        });
    }

    fn pop_queue_item(&mut self) {
        let queue_item = self.queue.pop().expect("at least one queued item");
        match queue_item.item_type {
            Type::SegmentId(seg_id) => {
                let opt_rc = self.source.get_segment(seg_id);
                if let Some(rc) = opt_rc {
                    self.add_segment(rc);
                } else {
                    error!("Couldn't get segment {:?} from source", seg_id);
                }
            }
            Type::Segment(rc) => {
                //TODO add every block in the segment, not just the cached ones
                let segment = &*rc;
                for block_num in 0..segment.num_blocks {
                    let block_id = (segment.id.0, segment.id.1, block_num);
                    self.add_block_id(block_id);
                }
            }
            Type::BlockId(block_id) => {
                let opt_rc = self.source.get_block(block_id);
                if let Some(rc) = opt_rc {
                    self.add_block(rc);
                } else {
                    error!("Couldn't get block {:?} from source", block_id);
                }
            }
            Type::Block(rc) => {
                let mut iter = Block::iter(&rc);

                /* Get the first row in this block; if there isn't one, skip the block entirely.
                   Otherwise, set it as the next start point if necessary.
                 */
                let current = iter.next();
                if current.is_none() {
                    return;
                }

                debug!("Begin block starting at {:?}", current);
                self.live.push(LiveItem {
                    iter,
                    current,
                    txn_id: TransactionId::MAX
                });
            }
        }

    }

    fn check_queue(&mut self, current: &[Datum]) {
        debug!("Checking for queue for stuff to become live");
        while let Some(next_queue_item) = self.queue.peek() {
            /* If we already have one and the first queued thing starts after it, do nothing. */
            if compare_points(self.num_dims,&next_queue_item.start_point, current).is_gt() {
                return;
            }

            /* Otherwise pop at least one queued thing. */
            self.pop_queue_item();
        }
    }
}

impl<'txn> Iterator for Scan<'txn> {
    type Item = QueryRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut current = self.queue.peek().map(|x| x.start_point.clone());
            let mut need_to_deqeue = true;
            debug!("Current is {:?}", current);

            /* Find the row in the current live set with the lowest point; if the lowest is equal to the
               next queued thing, then we need to dequeue at least one thing. */
            for item in &self.live {
                if current.is_none() || compare_points(self.num_dims, item.current.as_ref().unwrap(), current.as_ref().unwrap()).is_lt() {
                    need_to_deqeue = false;
                    current = item.current.clone();
                }
            }

            let current_point = current.as_ref()?;

            if need_to_deqeue {
                self.check_queue(current_point);
                continue;
            }

            if self.live.is_empty() && !self.queue.is_empty() {
                continue;
            }

            /* Now check everything that's live for the best thing to return. */
            let mut best_txn_id = 0;
            let mut best_row: Option<Vec<Datum>> = None;
            debug!("Current is {:?}", current_point);
            debug!("Looking for best row in {:?} live iterators", self.live.len());
            for item in self.live.iter_mut() {
                let item_point = item.current.as_ref().unwrap();
                debug!("Iterator current is {:?} from txn {:?}", item_point, item.txn_id);
                if compare_points(self.num_dims, item_point, current_point).is_eq() {
                    if item.txn_id > best_txn_id {
                        best_txn_id = item.txn_id;
                        best_row = Some(item.current.as_ref().unwrap().clone());
                        item.current = item.iter.next();
                    } else {
                        debug!("Ignoring row {:?} from txn {:?}", item_point, item.txn_id);
                        item.current = item.iter.next();
                    }
                }
            }
            debug!("Best row found was {:?}", best_row);

            /* Clean up the live set. */
            self.live.retain(|x| x.current.is_some());

            return best_row.map(|x| QueryRow { txn_id: best_txn_id, values_array: x });
        }
    }
}

impl<'txn> PartialEq<Self> for QueuedItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<'txn> Eq for QueuedItem {}

impl<'txn> PartialOrd<Self> for QueuedItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'txn> Ord for QueuedItem {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_points(self.start_point.len(), &self.start_point, &other.start_point).reverse()
    }
}

#[cfg(test)]
mod scan_tests {
    use std::collections::HashMap;
    use super::*;

    struct MemSource {
        segments: HashMap<(TransactionId, SegmentId), Rc<Segment>>
    }

    impl MemSource {
        fn new<'t>() -> Box<dyn ScanSource + 't> {
            Box::new(MemSource { segments: HashMap::new() })
        }
    }

    impl ScanSource for MemSource {
        fn get_segment(&self, seg_id: SegmentId) -> Option<Rc<Segment>> {
            todo!()
        }
        fn get_block(&self, block_id: BlockId) -> Option<Rc<Block>> {
            todo!()
        }
    }

    #[test]
    fn empty_scan() {
        let source = MemSource::new();
        let mut scan = Scan::new(source, 2, 5);

        assert!(&scan.next().is_none());
    }

    #[test]
    fn one_empty_local_block() {
        let b = Rc::new(Block::new(2));

        let source = MemSource::new();
        let mut scan = Scan::new(source, 2, 5);
        scan.add_block(b);

        assert!(&scan.next().is_none());
    }

    #[test]
    fn one_local_block() {
        let mut b = Block::new(2);
        b.add_row(&[7, 4, 99]);
        b.add_row(&[9, 0, 101]);
        let b = Rc::new(b);

        let source = MemSource::new();
        let mut scan = Scan::new(source, 2, 5);
        scan.add_block(b);

        let r = scan.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 7);
        assert_eq!(r[1], 4);
        assert_eq!(r[2], 99);
        assert_eq!(r.txn_id, TransactionId::MAX);

        let r = scan.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 9);
        assert_eq!(r[1], 0);
        assert_eq!(r[2], 101);
        assert_eq!(r.txn_id, TransactionId::MAX);

        assert!(&scan.next().is_none());
    }

    #[test]
    fn two_local_blocks() {
        let mut b = Block::new(2);
        b.add_row(&[7, 4, 99]);
        let b = Rc::new(b);
        let mut b2 = Block::new(2);
        b2.add_row(&[9, 0, 101]);
        let b2 = Rc::new(b2);

        let source = MemSource::new();
        let mut scan = Scan::new(source, 2, 5);
        scan.add_block(b);
        scan.add_block(b2);

        let r = scan.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 7);
        assert_eq!(r[1], 4);
        assert_eq!(r[2], 99);

        let r = scan.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 9);
        assert_eq!(r[1], 0);
        assert_eq!(r[2], 101);

        assert!(&scan.next().is_none());
    }
}
