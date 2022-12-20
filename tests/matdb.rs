use std::path::Path;
use std::time::Instant;

use matdb::{Database, Dimension, Value, Schema, Transaction};

fn create_database() -> Database {
    let mut database_path = std::env::temp_dir();
    database_path.push(Path::new("testdb"));

    let matdb;
    if database_path.exists() {
        matdb = Database::open(database_path.as_path()).unwrap();
    } else {
        matdb = Database::create(Schema {
            dimensions: vec![
                Dimension { name: String::from("time"), chunk_size: 50 },
                Dimension { name: String::from("sensor_id"), chunk_size: 10 },
            ],
            values: vec![
                Value { name: String::from("value") }
            ]
        }, database_path.as_path()).unwrap();
    }

    matdb
}

fn insert_data(txn: &mut Transaction) {
    let mut count = 0;
    let now = Instant::now();
    for i in 0..100 {
        for j in 0..100 {
            txn.add_row(&[i, j, i*1000 + j]);
            count += 1;
        }
        if i % 100 == 0 {
            txn.flush().unwrap();
        }
    }
    println!("Inserted {} rows in {:?}", count, now.elapsed());
}

fn query_data(txn: &Transaction) {
    let mut count = 0;
    let now = Instant::now();
    for _row in txn.query() {
        //println!("Row: {:?}", row);
        count += 1;
    }
    println!("Queried {} rows in {:?}", count, now.elapsed());
    assert_eq!(count, 10000);
}

#[test]
fn main() {
    stderrlog::new()
        .verbosity(3)
        .init().unwrap();

    let mut matdb = create_database();

    let mut txn = matdb.new_transaction().unwrap();

    insert_data(&mut txn);
    query_data(&txn);

    let now = Instant::now();
    txn.commit().unwrap();
    println!("Committed in {:?}", now.elapsed());

    let txn2 = matdb.new_transaction().unwrap();
    query_data(&txn2);

    txn2.rollback();

    println!("Done");
}
