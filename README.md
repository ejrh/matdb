MatDB
====

[![Rust build](https://github.com/ejrh/matdb/actions/workflows/rust-build.yml/badge.svg)](https://github.com/ejrh/matdb/actions/workflows/rust-build.yml)
[![Rust tests](https://github.com/ejrh/matdb/actions/workflows/rust-tests.yml/badge.svg)](https://github.com/ejrh/matdb/actions/workflows/rust-tests.yml)
[![Rust Clippy](https://github.com/ejrh/matdb/actions/workflows/rust-clippy.yml/badge.svg)](https://github.com/ejrh/matdb/actions/workflows/rust-clippy.yml)

MatDB is a very simple database designed for storing simple arrays of numbers, particularly fixed size integers.

While it can store sparse matrices, it will perform much better on data that is key-dense.

In theory, this should make it suitable for storing timeseries data.

Installation
---

MatDB is a library, so installation means making it available as a library for other
Rust codebases.

As it is a pre-alpha work-in-progress, MatDB is not yet on `crates.io`.

To install for development purposes, use the following steps:

 1. `git clone` the repository
 2. Run `cargo build` in the repository directory

Example use
---

The public API is accessed through the `Database` struct, respresenting a currently open MatDB database.
A database can be created for the first time with `Database::create` function, or an existing
database can be opened with `Database::open`.  Both functions return a `Database` that can then
be used for inserting new data or querying data.

`Database::create` also takes a `Schema` struct describing the dimension and value columns that
the database will contain.
   
    let database_path = Path::new("my-example-database");
    let mut matdb = if database_path.exists() {
        Database::open(database_path)
    } else {
        Database::create(Schema {
            dimensions: vec![
                Dimension { name: String::from("x"), chunk_size: 1000 },
                Dimension { name: String::from("y"), chunk_size: 1000 },
            ],
            values: vec![
                Value { name: String::from("value")}
            ]
        }, database_path)
    }

All updates and queries on the database are done through `Transaction` structs.  Only data
inserted by previously committed transactions, and the current transaction, are visible to
queries within the transaction.

    // Insert a row
    // Columns:   x             y               value
    txn.add_row(&[113, 47, 5]);

Queries are done through iteration over the entire visible database content.

    // Should print x=113 y=47 value=5
    for row in txn.query() {
        println!("x={} y={} value={}", row[0], row[1], row[2]);
    }

When the `Transaction` is committed its changes will be made permanent and become visible
to future transactions.  If the `Transaction` is instead rolled back, its changes are
discarded; this is the default when the `Transaction` lifetime ends.

    let mut txn = matdb.new_transaction().unwrap();

    txn.commit().unwrap();

    // Or rollback to discard changes.
    // txn.rollback().unwrap();

Current State
---

**MatDB is a work in progress and not ready for production use!**

Opens transactions, allows data to be inserted, saves the data to disk temporarily,
and permanently when the transaction is committed.

Within a transaction, allows data to be queried.  Returns data that was committed
at the time the transaction was opened, and data inserted within the transaction.
Returns only the most recent copy of any given row matching a point.

This document describes the currently *envisioned* design and implementation.  Hence,
the current MatDB program may behave quite differently from what is expected.

There is a test program called "sensor-log" that imports timeseries data from a text file.

Concepts
---

*Dimension* - A column that forms part of the key for each row.

*Value* - A column that is not part of the key.

*Row* - A group of column values corresponding to the values at a certain key. 

*Segment* - A single database file consisting of a sequence of blocks written in a single transaction.

*Block* - A conveniently-sized set of rows with similar keys.

*Transaction* - A transient view of a database in which updates and queries can be made.  If updates are made, the transaction must be committed for these to be visible outside the transaction.

*Database* - A set of rows, stored as a set of segments in a directory on disk. 

*Schema* - A description of the keys and values in a database, and some parameters for how to organise them for efficiency.  The schema *cannot* be changed after database creation.
