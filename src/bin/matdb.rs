use std::path::Path;
use std::time::Instant;
use matdb;
use matdb::{Database, Datum, Dimension, QueryRow, Schema, Transaction, Value};

fn create_database() -> Database {
    let database_path = Path::new("testdb");

    let mut matdb;
    if database_path.exists() {
        matdb = matdb::Database::open(database_path).unwrap();
        println!("Opened database");
    } else {
        matdb = matdb::Database::create(matdb::Schema {
            dimensions: vec![
                Dimension { name: String::from("time"), chunk_size: 500 },
                Dimension { name: String::from("sensor_id"), chunk_size: 100 },
            ],
            values: vec![
                Value { name: String::from("value") }
            ]
        }, database_path).unwrap();
        println!("Created database");
    }

    for dim in &matdb.schema.dimensions {
        println!("    Dim {:?} {:?}", dim.name, dim.chunk_size);
    }
    for val in &matdb.schema.values {
        println!("    Val {:?}", val.name);
    }

    matdb
}

fn insert_data(txn: &mut Transaction) {
    let mut count = 0;
    let now = Instant::now();
    for i in 0..1000 {
        for j in 0..1000 {
            txn.add_row(&[i, j, i*1000 + j]);
            count += 1;
        }
    }
    println!("Inserted {} rows in {:?}", count, now.elapsed());
}

fn query_data(txn: &Transaction) {
    let mut count = 0;
    let now = Instant::now();
    let mut values_array: Vec<Datum> = Vec::new();
    for row in txn.query(&mut values_array) {
        //println!("Row: {:?}", row);
        count += 1;
    }
    println!("Queried {} rows in {:?}", count, now.elapsed());
}

fn main() {
    let mut matdb = create_database();

    let mut txn = matdb.new_transaction().unwrap();

    insert_data(&mut txn);
    query_data(&txn);

    let now = Instant::now();
    txn.commit();
    println!("Committed in {:?}", now.elapsed());

    let mut txn2 = matdb.new_transaction().unwrap();
    query_data(&txn2);

    txn2.rollback();

    println!("Done");
}
