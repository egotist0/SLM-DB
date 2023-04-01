A high-performance standalone KV STORAGE ENGIN merging the high write performance of LSM Tree and the read ability of B+ Tree.

## Attributes:

* **Store Records’ FileID and Offset on corresponding SSTable in in-memory B+ tree to speed up IO. Set WAL for persistence.**
* **Define a multi-level Memtable slice based on a skipList for sorting and periodic flush when data is written.**
* **Get rid of the redundancy of Multiple-Level SSTable, Implement a single-level SSTable, use SLB-DB’s Selective Compaction strategy to manage GC and range search.**

## Overview
Inspired by a paper named [SLM-DB](https://www.usenix.org/conference/fast19/presentation/kaiyrakhmet) in USENIX FAST ’19, and the [Wisckey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) paper also helps a lot.


## Quick Start



## License
egoDB is under the Apache 2.0 license.

