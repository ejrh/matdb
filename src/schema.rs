use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use crate::{BlockKey, Datum, Dimension, Error, Value};
use crate::storage::SCHEMA_FILENAME;

use serde::{Serialize, Deserialize};
use serde_json;

#[derive(Serialize, Deserialize, Debug)]
pub struct Dimension {
    pub name: String,
    pub chunk_size: usize
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub dimensions: Vec<Dimension>,
    pub values: Vec<Value>,
}

impl Schema {
    pub(crate) fn get_chunk_key(&self, values: &[Datum]) -> BlockKey {
        let mut key_values : Vec<Datum> = Vec::new();

        for (dim_no, dim) in self.dimensions.iter().enumerate() {
            let dim_value = values[dim_no];
            let key_value = dim_value / dim.chunk_size;
            key_values.push(key_value);
        }

        BlockKey { key_values }
    }

    pub(crate) fn load(database_path: &Path) -> Result<Schema, Error> {
        let schema_filename = database_path.join(SCHEMA_FILENAME);
        let mut file = File::open(schema_filename)?;
        let mut json = String::new();
        file.read_to_string(&mut json)?;
        let schema: Schema = serde_json::from_str(json.as_str())?;
        Ok(schema)
    }

    pub(crate) fn save(&self, database_path: &Path) -> Result<(), Error> {
        let schema_filename = database_path.join(SCHEMA_FILENAME);
        let mut file = File::create(schema_filename)?;
        let json = serde_json::to_string(&self)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
}
