use std::collections::hash_map;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::zip;
use std::ops::Index;

use serde::{Serialize, Deserialize};

mod block;
mod database;
mod segment;
mod schema;
mod storage;
mod transaction;

use crate::block::{Block, BlockIter};
pub use crate::database::Database;
pub use crate::schema::Schema;
pub use crate::transaction::Transaction;


#[derive(Debug)]
pub enum Error {
    IoError,
    SchemaError,
    DataError
}

pub type Datum = usize;

pub type TransactionId = u32;
pub type SegmentId = u16;

#[derive(Serialize, Deserialize, Debug)]
pub struct Value {
    pub name: String
}

pub struct BlockKey {
    key_values : Vec<Datum>
}

pub struct QueryRow {
    values_array: *const Vec<Datum>
}

pub struct QueryIterator<'txn> {
    block_iter: hash_map::Iter<'txn, BlockKey, Block>,
    block_key: Option<&'txn BlockKey>,
    value_iter: Option<BlockIter<'txn>>,
    values_array: *mut Vec<Datum>
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Error::IoError
    }
}

impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Error::IoError
    }
}

impl Eq for BlockKey { }

impl PartialEq<Self> for BlockKey {
    fn eq(&self, other: &Self) -> bool {
        for (&a, &b) in zip(&self.key_values, &other.key_values) {
            if a != b { return false }
        }
        true
    }
}

impl Hash for BlockKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for &v in &self.key_values {
            state.write_usize(v);
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
    fn new(values_array: *const Vec<Datum>) -> Self {
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
