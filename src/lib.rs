use std::collections::{hash_map, HashMap};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::iter::zip;
use std::ops::Index;
use std::path::{Path, PathBuf};

use serde::{Serialize, Deserialize};
use serde_json;
use zstd::zstd_safe;

#[derive(Debug)]
pub enum Error {
    IoError,
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

pub struct BlockKey {
    key_values : Vec<Datum>
}

pub struct Database {
    pub path: PathBuf,
    pub schema: Schema
}

pub struct Transaction<'db> {
    database: &'db Database,
    blocks: HashMap<BlockKey, Block>
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

mod block;

use crate::block::{Block, BlockIter};

mod storage;

use crate::storage::{read_tag, skip_to_next_tag, write_tag};
use crate::storage::Tag::{BlockTag, EndTag};

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        return Error::IoError;
    }
}

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
            blocks: Default::default()
        })
    }
}

impl<'db> Transaction<'db> {
    pub fn add_row(&mut self, values: &[Datum]) -> Result<(), Error> {
        let key = self.database.schema.get_chunk_key(values);
        let ref mut block = self.blocks.entry(key).or_insert_with(|| Block::new(self.database.schema.dimensions.len()));
        block.add_row(values);
        Ok(())
    }

    pub fn commit(mut self) {
        // Consume the Transaction, if nothing else
    }

    pub fn query(&'db self, values_array: &mut Vec<Datum>) -> QueryIterator<'db> {
        QueryIterator {
            block_iter: self.blocks.iter(),
            block_key: None,
            value_iter: None,
            values_array
        }
    }

    pub fn load(&mut self) -> Result<(), Error> {
        let file = File::open("test")?;
        let mut src = BufReader::with_capacity(zstd_safe::DCtx::in_size(), file);

        loop {
            let tag = read_tag(&mut src);

            match tag {
                BlockTag => self.load_block(&mut src)?,
                EndTag => break
            }
        }

        Ok(())
    }

    pub fn load_block(&mut self, src: &mut BufReader<File>) -> Result<(), Error> {
        let mut block = Block::new(0);

        block.load(src)?;

        /* Pick the first row and use it as the chunk for for the whole block */
        let mut values_array: Vec<Datum> = Vec::new();
        let first = block.iter(&mut values_array).next();
        if first.is_none() { return Ok(()); }
        let key = self.database.schema.get_chunk_key(&values_array);

        self.blocks.insert(key, block);

        /* ZStd leaves the last byte of a stream in the buffer, meaning we cant just read any other
           data after it.  This seems to be the "hostage byte" in the decompressor:
           https://github.com/facebook/zstd/blob/dev/lib/decompress/zstd_decompress.c#L2238
           To work around it, we scan for something that looks like a tag.  If there is only
           ever one byte to skip over, we should be able to do this unambiguously.  If not...?
         */
        skip_to_next_tag(src)?;

        Ok(())
    }

    pub fn save(&mut self) -> Result<(), Error> {
        let mut file = File::create("test")?;

        for buf in self.blocks.values() {
            write_tag(&mut file, BlockTag)?;
            buf.save(&mut file)?;
        }

        write_tag(&mut file, EndTag)?;

        Ok(())
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

impl Schema {
    fn get_chunk_key(&self, values: &[Datum]) -> BlockKey {
        let mut key_values : Vec<Datum> = Vec::new();

        for (dim_no, dim) in self.dimensions.iter().enumerate() {
            let dim_value = values[dim_no];
            let key_value = dim_value / dim.chunk_size;
            key_values.push(key_value);
        }

        BlockKey { key_values }
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

impl Debug for QueryRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let va = unsafe { self.values_array.as_ref() }.unwrap();
        f.debug_list().entries(va).finish()
   }
}
