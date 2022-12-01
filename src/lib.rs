extern crate core;

use std::collections::{hash_map, HashMap};
use std::hash::{Hash, Hasher};
use std::iter::zip;
use std::ops::Index;
use std::path::{Path, PathBuf};

use serde::{Serialize, Deserialize};
use serde_json;

#[derive(Debug)]
pub enum Error {
    IOError,
    SchemaError,
    DataError
}

pub type Datum = usize;

#[derive(Serialize, Deserialize, Debug)]
pub struct Dimension {
    pub name: String,
    pub chunk_size: usize
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Value {
    pub name: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub dimensions: Vec<Dimension>,
    pub values: Vec<Value>,
}

pub struct ChunkKey {
    key_values : Vec<Datum>
}

pub struct Database {
    pub path: PathBuf,
    pub schema: Schema
}

pub struct Transaction<'db> {
    database: &'db Database,
    buffers: HashMap<ChunkKey, Buffer>
}

#[derive(Debug)]
pub struct QueryRow {
    values_array: *const Vec<Datum>
}

pub struct QueryIterator<'txn> {
    buffer_iter: hash_map::Iter<'txn, ChunkKey, Buffer>,
    buffer_key: Option<&'txn ChunkKey>,
    value_iter: Option<BufferIter<'txn>>,
    values_array: *mut Vec<Datum>
}

mod buffer;

use crate::buffer::{Buffer, BufferIter};

impl Database {
    pub fn create(schema: Schema, path: &Path) -> Result<Database, Error> {
        let j = serde_json::to_string(&schema).unwrap();
        Ok(Database {
            path: path.to_path_buf(),
            schema
        })
    }

    pub fn open(path: &Path) -> Result<Database, Error> {
        let schema = serde_json::from_str("{}").unwrap();

        Ok(Database {
            path: path.to_path_buf(),
            schema
        })
    }

    pub fn new_transaction<'db>(&'db mut self) -> Result<Transaction<'db>, Error> {
        Ok(Transaction {
            database: self,
            buffers: Default::default()
        })
    }
}

impl<'db> Transaction<'db> {
    pub fn add_row(&mut self, values: &[Datum]) -> Result<(), Error> {
        let key = self.database.schema.get_chunk_key(values);
        let ref mut buffer = self.buffers.entry(key).or_insert_with(|| Buffer::new(self.database.schema.dimensions.len()));
        buffer.add_row(values);
        Ok(())
    }

    pub fn commit(mut self) {
        // Consume the Transaction, if nothing else
    }

    pub fn query(&'db self, values_array: &mut Vec<Datum>) -> QueryIterator<'db> {
        QueryIterator {
            buffer_iter: self.buffers.iter(),
            buffer_key: None,
            value_iter: None,
            values_array
        }
    }
}

impl Eq for ChunkKey { }

impl PartialEq<Self> for ChunkKey {
    fn eq(&self, other: &Self) -> bool {
        for (&a, &b) in zip(&self.key_values, &other.key_values) {
            if a != b { return false }
        }
        true
    }
}

impl Hash for ChunkKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for &v in &self.key_values {
            state.write_usize(v);
        }
    }
}

impl Schema {
    fn get_chunk_key(&self, values: &[Datum]) -> ChunkKey {
        let mut key_values : Vec<Datum> = Vec::new();

        for (dim_no, dim) in self.dimensions.iter().enumerate() {
            let dim_value = values[dim_no];
            let key_value = dim_value / dim.chunk_size;
            key_values.push(key_value);
        }

        ChunkKey { key_values }
    }
}

impl<'txn> Iterator for QueryIterator<'txn> {
    type Item = QueryRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.value_iter.is_none() {
                let (buffer_key, buffer) = self.buffer_iter.next()?;
                self.buffer_key = Some(buffer_key);
                self.value_iter = Some(buffer.iter(unsafe { self.values_array.as_mut().unwrap() }));
            }

            let mut iter = self.value_iter.as_mut().unwrap();

            loop {
                match iter.next() {
                    Some(row) => {
                        return Some(row);
                    }
                    None => { break; }
                }
            }

            self.value_iter = None;
        }
    }
}

impl QueryRow {
    fn new(values_array: *const Vec<Datum>) -> Self {
        return QueryRow {
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
