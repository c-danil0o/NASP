// Functions needed for writing logs
package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"time"
)

// Writes single record
func (record *LogRecord) Write(writer io.Writer) error {
	// Making CRC hash
	record.CRC = crc32.ChecksumIEEE(record.Key)

	// Getting current time
	timestamp := time.Now().UnixNano()
	binary.PutVarint(record.Timestamp[:], timestamp)

	// Getting length of key
	keyLen := uint64(len(record.Key))
	record.KeySize = keyLen

	// Getting length of value
	valueLen := uint64(len(record.Value))
	record.ValueSize = valueLen

	// Create a buffer for the record
	var buf bytes.Buffer

	// Write the record to the buffer
	binary.Write(&buf, binary.BigEndian, record.CRC)
	binary.Write(&buf, binary.BigEndian, record.Timestamp)
	binary.Write(&buf, binary.BigEndian, record.Tombstone)
	binary.Write(&buf, binary.BigEndian, record.KeySize)
	binary.Write(&buf, binary.BigEndian, record.ValueSize)
	binary.Write(&buf, binary.BigEndian, record.Key)
	binary.Write(&buf, binary.BigEndian, record.Value)

	// Write the buffer to the file
	_, err := writer.Write(buf.Bytes())

	return err
}

// Main function for writing record
func (wal *SegmentedWAL) writeWal(record LogRecord) error {
	// There are more segments in current file than allowed
	if wal.currentSize+1 > MAX_SEGMENT_SIZE {
		// Closing current segment
		if err := wal.currentSegment.Close(); err != nil {
			return err
		}

		// Opening new segment
		segmentName := WAL_STR + strconv.Itoa(wal.segmentCount) + LOG_STR
		var err error
		wal.currentSegment, err = os.OpenFile(segmentName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		wal.segmentCount++
		wal.currentSize = 0
	}
	// Write the record to the file
	if err := record.Write(wal.currentSegment); err != nil {
		return err
	}

	// Flush the file
	if err := wal.currentSegment.Sync(); err != nil {
		return err
	}
	return nil
}
