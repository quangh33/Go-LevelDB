# Go-LevelDB - A [LevelDB](https://github.com/google/leveldb)-Inspired Key-Value Store

Go-LevelDB is a key-value storage engine built from scratch in Go,
inspired by the design of Google's [LevelDB](https://github.com/google/leveldb) and the **Log-Structured Merge-Tree (LSM-Tree)** architecture.

## Core Features ✨
- [x] **Durable & Fast Writes** All writes are first committed to a Write-Ahead Log (WAL) for durability, then placed 
in an in-memory Memtable for high-speed write performance.
- [x] **Sorted In-Memory Storage:** Uses a Skip List for the Memtable, keeping keys sorted at all times for efficient 
flushing and enabling range scans.
- [x] **Persistent, Immutable Storage**: Flushes full Memtables to immutable, sorted SSTable (.sst) files on disk
- [x] **Efficient Lookups**: SSTables are highly structured with block-based indexes and Bloom filters to minimize disk
reads, especially for non-existent keys.
- [ ] **Automatic Compaction** process to merge SSTables, reclaim space, and optimize read performance.

## Architecture Overview

![img.png](img/overview.png)

### WAL entry format

![](img/wal_format.png)

### SSTable format

![](img/sstable_format.png)

### Flush Memtable flow

![](img/flush.png)


## How to Run
1. Run the main program
```bash
go run .
```