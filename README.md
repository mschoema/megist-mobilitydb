ME-GiST Indexing for MobilityDB
===============================

This directory contains an implementation of ME-GiST indexes for MobilityDB data types.

Dependencies
------------
- [PostgreSQL 15](https://www.postgresql.org/)
- [MobilityDB 1.1 (latest version of the develop branch)](https://github.com/MobilityDB/MobilityDB)
- [MEOS (latest version of the develop branch of MobilityDB)](https://www.libmeos.org/)
- [ME-GiST](https://github.com/mschoema/megist)

Installation
------------
Compiling and installing the extension
```
make
sudo make install
```

Using the extension to create a Multi-Entry R-Tree on the tgeompoint column `trip` from the table `trips(id, trip)`
```sql
CREATE INDEX trips_megist_trip on trips using megist(trip);
```

Author:
	Maxime Schoemans	<maxime.schoemans@ulb.be>
