MatDB
====

[![Rust build and automated tests](https://github.com/ejrh/matdb/actions/workflows/rust.yml/badge.svg)](https://github.com/ejrh/matdb/actions/workflows/rust.yml)

MatDB is a very simple database designed for storing simple arrays of numbers, particularly fixed size integers.

While it can store sparse matrices, it will perform much better on data that is key-dense.

In theory, this should make it suitable for storing timeseries data.

Current State
---

Puts numbers in memory and reads them out again.

Saves data to disk and loads it again.

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
