# Go-LevelDB - A [LevelDB](https://github.com/google/leveldb)-Inspired Key-Value Store

Go-LevelDB is a key-value storage engine built from scratch in Go,
inspired by the design of Google's [LevelDB](https://github.com/google/leveldb) and the **Log-Structured Merge-Tree (LSM-Tree)** architecture.

## Core Features ✨
- [x] **Durable Writes** via a Write-Ahead Log (WAL).
- [ ] **Fast In-Memory Writes** using a Memtable.
- [ ] **Persistent, Immutable Storage** with on-disk SSTables (Sorted String Tables).
- [ ] **Efficient Lookups** using block-based indexing within SSTables.
- [ ] **Reduced Disk I/O** for non-existent keys via Bloom Filters.
- [ ] **Automatic Compaction** process to merge SSTables, reclaim space, and optimize read performance.

## How to Run
1. Run the main program
```bash
go run cmd/main.go
```