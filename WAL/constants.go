package WAL

import (
	"os"
)

const WAL_STR = "wal-"
const LOG_STR = ".log"

// Single WAL record
type LogRecord struct {
	CRC       uint32
	Timestamp [16]byte
	Tombstone byte
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

// Entire WAL that's segmented
type SegmentedWAL struct {
	CurrentSegment *os.File
	SegmentCount   int
	CurrentSize    int
	SegmentSize    int
}
