use std::cmp::Ordering;
use std::fmt::{Debug};
use std::hash::{Hash, Hasher};
use std::iter::zip;

mod block;
mod cache;
mod database;
mod query;
mod segment;
mod scan;
mod schema;
mod storage;
mod transaction;

pub use crate::database::Database;
pub use crate::schema::{Dimension, Value, Schema};
pub use crate::transaction::Transaction;

#[derive(Debug)]
pub enum Error {
    IoError,
    SchemaError,
    DataError
}

pub type Datum = usize;

pub type TransactionId = u32;
pub type SegmentNum = u16;
pub type BlockNum = u16;

pub type SegmentId = (TransactionId, SegmentNum);
pub type BlockId = (TransactionId, SegmentNum, BlockNum);

pub struct BlockKey {
    key_values : Vec<Datum>
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

pub(crate) fn compare_points(num_dims: usize, point1: &[Datum], point2: &[Datum]) -> Ordering {
    Ord::cmp(&point1[0..num_dims], &point2[0..num_dims])
}
