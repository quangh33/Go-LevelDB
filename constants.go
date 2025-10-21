package main

const (
	// DataBlockSize groups key-value pairs into blocks of this size.
	DataBlockSize         = 4096 // 4 KB
	SSTableCountThreshold = 10
	MemtableSizeThreshold = 4 * 1024 * 1024 // 4 MB
	TableCacheSize        = 128             // Number of SSTable readers to keep in cache
	BlockCacheSize        = 8 * 1024 * 1024 // 8MB block cache
)
