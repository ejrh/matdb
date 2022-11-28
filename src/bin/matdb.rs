use std::path::Path;
use std::time::Instant;
use matdb;
use matdb::{Dimension, Schema, Value};

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
    let elapsed = now.elapsed();
    println!("Inserted {} rows in {:?}", count, elapsed);

    let mut count = 0;
    let now = Instant::now();
    for row in txn.query() {
        //println!("Row: {:?}", row);
        count += 1;
    }
    let elapsed = now.elapsed();
    println!("Queried {} rows in {:?}", count, elapsed);

    txn.commit();

    println!("Goodbye");
}
