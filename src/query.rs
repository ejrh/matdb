use std::collections::{hash_map, HashMap};
use std::fmt::{Debug, Formatter};
use std::ops::Index;
use crate::block::{Block, BlockIter};
use crate::{BlockKey, Datum};

pub struct QueryRow {
    values_array: *const Vec<Datum>
}

pub struct QueryIterator<'txn> {
    block_iter: hash_map::Iter<'txn, BlockKey, Block>,
    block_key: Option<&'txn BlockKey>,
    value_iter: Option<BlockIter<'txn>>,
    values_array: *mut Vec<Datum>
}

impl<'txn> QueryIterator<'txn> {
    pub(crate) fn new(
        blocks: &'txn HashMap<BlockKey, Block>,
        values_array: &mut Vec<Datum>
    ) -> QueryIterator<'txn> {
        QueryIterator {
            block_iter: blocks.iter(),
            block_key: None,
            value_iter: None,
            values_array
        }
    }
}

impl<'txn> Iterator for QueryIterator<'txn> {
    type Item = QueryRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.value_iter.is_none() {
                let (block_key, block) = self.block_iter.next()?;
                self.block_key = Some(block_key);
                self.value_iter = Some(block.iter(unsafe { self.values_array.as_mut().unwrap() }));
            }

            let iter = self.value_iter.as_mut().unwrap();

            match iter.next() {
                Some(row) => {
                    return Some(row);
                }
                None => {
                    self.value_iter = None;
                }
            }
        }
    }
}

impl QueryRow {
    pub(crate) fn new(values_array: *const Vec<Datum>) -> Self {
        QueryRow {
            values_array
        }
    }
}

impl Index<usize> for QueryRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        let vals = unsafe { self.values_array.as_ref() };
        &vals.unwrap()[index]
    }
}

impl Debug for QueryRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let va = unsafe { self.values_array.as_ref() }.unwrap();
        f.debug_list().entries(va).finish()
   }
}
