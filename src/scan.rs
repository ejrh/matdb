use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;

use crate::block::{Block, BlockIter};
use crate::{BlockId, compare_points, Datum, SegmentId, TransactionId};
use crate::query::QueryRow;
use crate::segment::Segment;

pub(crate) enum Type<'txn> {
    SegmentId(SegmentId),
    Segment(&'txn Segment),
    BlockId(BlockId),
    Block(&'txn Block),
}

pub(crate) struct QueuedItem<'txn> {
    start_point: Vec<Datum>,
    item_type: Type<'txn>
}

#[derive(Clone)]
pub(crate) struct LiveItem<'txn> {
    iter: BlockIter<'txn>,
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
    num_dims: usize,
    this_txn_id: TransactionId,
    queue: BinaryHeap<QueuedItem<'txn>>,
    live: Vec<LiveItem<'txn>>
}

impl<'txn> Scan<'txn> {
    pub(crate) fn new(num_dims: usize, txn_id: TransactionId) -> Scan<'txn> {
        Scan {
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

    pub(crate) fn add_segment(&mut self, segment: &'txn Segment) {
        let start_point = segment.cached_blocks.values().flat_map(|x| x.get_start_point()).min();
        if start_point.is_none() {
            return;
        }
        let start_point = start_point.unwrap();
        self.queue.push(QueuedItem {
            start_point,
            item_type: Type::Segment(segment)
        });
    }

    pub(crate) fn add_block(&mut self, block: &'txn Block) {
        let start_point = block.get_start_point();
        if start_point.is_none() {
            return;
        }
        let start_point = start_point.unwrap();
        self.queue.push(QueuedItem {
            start_point,
            item_type: Type::Block(block)
        });
    }

    fn check_queue(&mut self, current: &Vec<Datum>) {
        while let Some(next_queue_item) = self.queue.peek() {
            /* If we already have one and the first queued thing starts after it, do nothing. */
            if compare_points(self.num_dims,&next_queue_item.start_point, current).is_gt() {
                return;
            }

            /* Otherwise pop at least one queued thing. */
            let queue_item = self.queue.pop().unwrap();
            match queue_item.item_type {
                Type::SegmentId(_seg_id) => {
                    //TODO get the segment from the cache and add it
                    todo!();
                }
                Type::Segment(segment) => {
                    //TODO add every block in the segment, not just the cached ones
                    for block in segment.cached_blocks.values() {
                        self.add_block(block);
                    }
                }
                Type::BlockId(_block_id) => {
                    //TODO get the block from the cache and add it
                    todo!();
                }
                Type::Block(block) => {
                    let mut iter = block.iter();

                    /* Get the first row in this block; if there isn't one, skip the block entirely.
                       Otherwise, set it as the next start point if necessary.
                     */
                    let current = iter.next();
                    if current.is_none() {
                        continue;
                    }

                    self.live.push(LiveItem {
                        iter,
                        current,
                        txn_id: self.this_txn_id
                    });
                }
            }
        }
    }
}

impl<'txn> Iterator for Scan<'txn> {
    type Item = QueryRow;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current = self.queue.peek().map(|x| x.start_point.clone());
        let mut need_to_deqeue = true;

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
        }

        /* Now check everything that's live for the best thing to return. */
        let mut best_txn_id = 0;
        let mut best_row: Option<Vec<Datum>> = None;
        for item in self.live.iter_mut() {
            let item_point = item.current.as_ref().unwrap();
            if compare_points(self.num_dims, item_point, current_point).is_eq() && item.txn_id > best_txn_id {
                best_txn_id = item.txn_id;
                best_row = Some(item.current.as_ref().unwrap().clone());
                item.current = item.iter.next();
            }
        }

        /* Clean up the live set. */
        self.live.retain(|x| x.current.is_some());

        best_row.map(|x| QueryRow { txn_id: best_txn_id, values_array: x })
    }
}

impl<'txn> PartialEq<Self> for QueuedItem<'txn> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<'txn> Eq for QueuedItem<'txn> {}

impl<'txn> PartialOrd<Self> for QueuedItem<'txn> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'txn> Ord for QueuedItem<'txn> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_points(self.start_point.len(), &self.start_point, &other.start_point).reverse()
    }
}

#[cfg(test)]
mod block_sorter_tests {
    use super::*;

    #[test]
    fn empty_block_sorter() {
        let mut bs = Scan::new(2, 5);

        assert!(&bs.next().is_none());
    }

    #[test]
    fn one_empty_local_block() {
        let mut bs = Scan::new(2, 5);
        let b = Block::new(2);
        bs.add_block(&b);

        assert!(&bs.next().is_none());
    }

    #[test]
    fn one_local_block() {
        let mut bs = Scan::new(2, 5);

        let mut b = Block::new(2);
        b.add_row(&[7, 4, 99]);
        b.add_row(&[9, 0, 101]);
        bs.add_block(&b);

        let r = bs.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 7);
        assert_eq!(r[1], 4);
        assert_eq!(r[2], 99);
        assert_eq!(r.txn_id, 5);

        let r = bs.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 9);
        assert_eq!(r[1], 0);
        assert_eq!(r[2], 101);
        assert_eq!(r.txn_id, 5);

        assert!(&bs.next().is_none());
    }

    #[test]
    fn two_local_blocks() {
        let mut bs = Scan::new(2, 5);

        let mut b = Block::new(2);
        b.add_row(&[7, 4, 99]);
        bs.add_block(&b);

        let mut b = Block::new(2);
        b.add_row(&[9, 0, 101]);
        bs.add_block(&b);

        let r = bs.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 7);
        assert_eq!(r[1], 4);
        assert_eq!(r[2], 99);

        let r = bs.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(r[0], 9);
        assert_eq!(r[1], 0);
        assert_eq!(r[2], 101);

        assert!(&bs.next().is_none());
    }
}
