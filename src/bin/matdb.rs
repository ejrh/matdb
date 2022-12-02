use std::path::Path;
use std::time::Instant;
use matdb;
use matdb::{Datum, Dimension, QueryRow, Schema, Value};

fn main() {
    println!("Hello");

    let mut matdb = matdb::Database::create(matdb::Schema {
        dimensions: vec![
            Dimension { name: String::from("time"), chunk_size: 100 },
            Dimension { name: String::from("sensor_id"), chunk_size: 100 },
        ],
        values: vec![
            Value { name: String::from("value")}
        ]
    }, Path::new("testdb")).unwrap();

    println!("Created database");

    for dim in &matdb.schema.dimensions {
        println!("Dim {:?} {:?}", dim.name, dim.chunk_size);
    }
    for val in &matdb.schema.values {
        println!("Val {:?}", val.name);
    }

    let mut txn = matdb.new_transaction().unwrap();

    println!("Created transaction");

    let mut count = 0;
    let now = Instant::now();
    for i in 0..1000 {
        for j in 0..1000 {
            txn.add_row(&[i, j, i*1000 + j]);
            count += 1;
        }
    }
    println!("Inserted {} rows in {:?}", count, now.elapsed());

    let mut count = 0;
    let now = Instant::now();
    let mut values_array: Vec<Datum> = Vec::new();
    for row in txn.query(&mut values_array) {
        //println!("Row: {:?}", row);
        count += 1;
    }
    println!("Queried {} rows in {:?}", count, now.elapsed());

    let now = Instant::now();
    txn.save();
    println!("Saved in {:?}", now.elapsed());
    txn.commit();

    let mut txn = matdb.new_transaction().unwrap();
    let now = Instant::now();
    txn.load();
    println!("Loaded in {:?}", now.elapsed());

    let mut count = 0;
    let now = Instant::now();
    let mut values_array: Vec<Datum> = Vec::new();
    for row in txn.query(&mut values_array) {
        //println!("Row: {:?}", row);
        count += 1;
    }
    println!("Queried {} rows in {:?}", count, now.elapsed());

    println!("Goodbye");
}
