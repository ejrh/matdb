use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;
use std::iter::Peekable;
use std::mem::take;
use std::ptr::replace;

use crate::block::{Block, BlockIter};
use crate::{BlockNum, compare_points, Datum, SegmentId, Transaction, TransactionId};
use crate::query::QueryRow;

pub(crate) enum Type<'txn> {
    Segment(TransactionId, SegmentId),
    SegmentBlock(TransactionId, SegmentId, BlockNum),
    LocalBlock(&'txn Block),
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
    this_txn_id: TransactionId,
    queue: BinaryHeap<QueuedItem<'txn>>,
    live: Vec<LiveItem<'txn>>
}

impl<'txn> Scan<'txn> {
    pub(crate) fn new(txn_id: TransactionId) -> Scan<'txn> {
        Scan {
            this_txn_id: txn_id,
            queue: Default::default(),
            live: Default::default()
        }
    }

    pub(crate) fn add(&mut self, item: QueuedItem<'txn>) {
        self.queue.push(item)
    }

    fn check_queue(&mut self, current: &Vec<Datum>) {
        while let Some(next_queue_item) = self.queue.peek() {
            /* If we already have one and the first queued thing starts after it, do nothing. */
            if compare_points(2,&current, &next_queue_item.start_point).is_gt() {
                return;
            }

            /* Otherwise pop at least one queued thing. */
            let next_queue_item = self.queue.pop().unwrap();
            match next_queue_item.item_type {
                Type::Segment(txn_id, seg_id) => {
                    todo!();
                }
                Type::SegmentBlock(txn_id, seg_id, block_num) => {
                    todo!();
                }
                Type::LocalBlock(block) => {
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
                        current: current,
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
            if current.is_none() || compare_points(2, &item.current.as_ref().unwrap(), &current.as_ref().unwrap()).is_lt() {
                need_to_deqeue = false;
                current = item.current.clone();
            }
        }

        if current.is_none() {
            return None;
        }

        if need_to_deqeue {
            self.check_queue(&current.as_ref().unwrap());
        }

        /* Now check everything that's live for the best thing to return. */
        let mut best_txn_id = 0;
        let mut best_row: Option<Vec<Datum>> = None;
        for item in self.live.iter_mut() {
            if compare_points(2, &item.current.as_ref().unwrap(), &current.as_ref().unwrap()).is_eq() {
                if item.txn_id > best_txn_id {
                    best_txn_id = item.txn_id;
                    best_row = Some(item.current.as_ref().unwrap().clone());
                    item.current = item.iter.next();
                }
            }
        }

        /* Clean up the live set. */
        self.live.retain(|x| x.current.is_some());

        let rv = best_row.map(|x| QueryRow { txn_id: best_txn_id, values_array: x });
        println!("Return value {:?}", rv);
        rv
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
        Datum::cmp(&self.start_point[0], &other.start_point[0]).reverse()
    }
}

#[cfg(test)]
mod block_sorter_tests {
    use crate::block::Block;
    use crate::scan::{Scan, QueuedItem};
    use crate::scan::Type::LocalBlock;

    #[test]
    fn empty_block_sorter() {
        let mut bs = Scan::new(5);

        assert!(&bs.next().is_none());
    }

    #[test]
    fn one_empty_local_block() {
        let mut bs = Scan::new(5);
        let mut b = Block::new(2);

        assert!(&bs.next().is_none());
    }

    #[test]
    fn one_local_block() {
        let mut bs = Scan::new(5);

        let mut b = Block::new(2);
        b.add_row(&[7, 4, 99]);
        b.add_row(&[9, 0, 101]);
        bs.add(QueuedItem {
            start_point: vec![7, 4],
            item_type: LocalBlock(&b)
        });

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
        let mut bs = Scan::new(5);

        let mut b = Block::new(2);
        b.add_row(&[7, 4, 99]);
        bs.add(QueuedItem {
            start_point: vec![7, 4],
            item_type: LocalBlock(&b)
        });

        let mut b = Block::new(2);
        b.add_row(&[9, 0, 101]);
        bs.add(QueuedItem {
            start_point: vec![9, 0],
            item_type: LocalBlock(&b)
        });

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
