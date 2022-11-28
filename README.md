MatDB
====

MatDB is a very simple database designed for storing simple arrays of numbers, particularly fixed size integers.

While it can store sparse matrices, it will perform much better on data that is key-dense.

In theory, this should make it suitable for storing timeseries data.

Current State
---

Puts numbers in memory and reads them out again.

Concepts
---

*Dimension* - A column that forms part of the key for each row.

*Value* - A column that is not part of the key.

*Row* - A group of column values corresponding to the values at a certain key. 

*Segment* - A single database file consisting of a sequence of chunks.

*Chunk* - A conveniently-sized set of rows with similar keys.

*Transaction* - A transient view of a database in which updates and queries can be made.  If updates are made, the transaction must be committed for these to be visible outside the transaction.

*Database* - A set of rows, stored in a directory on disk. 

*Schema* - A description of the keys and values in a database, and some parameters for how to organise them for efficiency.  The schema *cannot* be changed after database creation.

