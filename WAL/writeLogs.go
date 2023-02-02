// Functions needed for writing logs
package WAL

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"time"
)

// Writes single record
func (record *LogRecord) Write(writer io.Writer) error {

	// Getting current time
	timestamp := time.Now().UnixNano()
	binary.PutVarint(record.Timestamp[:], timestamp)

	// Getting length of key
	keyLen := uint64(len(record.Key))
	record.KeySize = keyLen

	// Getting length of value
	valueLen := uint64(len(record.Value))
	record.ValueSize = valueLen

	// Making CRC hash
	var combined []byte
	combined = append(combined, record.Timestamp[:]...)
	combined = append(combined, record.Tombstone)
	combined = append(combined, byte(record.KeySize))
	combined = append(combined, byte(record.ValueSize))
	combined = append(combined, record.Key...)
	combined = append(combined, record.Value...)
	record.CRC = crc32.ChecksumIEEE(combined)

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
func (wal *SegmentedWAL) WriteRecord(record LogRecord) error {
	// Going to next segment
	if wal.CurrentSize+1 > wal.SegmentSize {
		wal.SegmentCount++
		wal.CurrentSize = 0
	}

	// Openning current file for segment
	var err error
	wal.CurrentSegment, err = os.OpenFile(WAL_STR+strconv.Itoa(wal.SegmentCount)+LOG_STR, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Write the record to the file
	if err := record.Write(wal.CurrentSegment); err != nil {
		fmt.Println(err)
		return err
	}

	// Flush the file
	if err := wal.CurrentSegment.Sync(); err != nil {
		fmt.Println(err)
		return err
	}
	wal.CurrentSize++
	return nil
}
