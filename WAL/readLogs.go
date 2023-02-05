// Functions needed for reading log files
package WAL

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/edsrzf/mmap-go"
)

// Reading single record
func ReadRecord(data mmap.MMap) (LogRecord, error) {
	record := &LogRecord{}

	var offset uint64
	record.CRC = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	copy(record.Timestamp[:], data[offset:offset+16])
	offset += 16

	record.Tombstone = data[offset]
	offset++

	record.KeySize = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	record.ValueSize = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	record.Key = make([]byte, record.KeySize)
	copy(record.Key, data[offset:offset+record.KeySize])
	offset += record.KeySize

	record.Value = make([]byte, record.ValueSize)
	copy(record.Value, data[offset:offset+record.ValueSize])
	offset += record.ValueSize

	var combined []byte
	combined = append(combined, record.Timestamp[:]...)
	combined = append(combined, record.Tombstone)
	combined = append(combined, byte(record.KeySize))
	combined = append(combined, byte(record.ValueSize))
	combined = append(combined, record.Key...)
	combined = append(combined, record.Value...)
	if record.CRC != crc32.ChecksumIEEE(combined) {
		return *record, fmt.Errorf("CRC verification failed")
	}

	return *record, nil
}

// Read the records from the byte slice
func AllRecords(data mmap.MMap) error {
	var offset uint64

	for offset < uint64(len(data)) {
		// Create a new LogRecord and fill with info
		record, err := ReadRecord(data[offset:])
		if err != nil {
			return err
		}
		offset += record.recordSize()

		// Print the record
		fmt.Println()
		fmt.Printf("CRC: %d\n", record.CRC)
		fmt.Printf("Timestamp: %d\n", record.Timestamp)
		fmt.Printf("Tombstone: %d\n", record.Tombstone)
		fmt.Printf("KeySize: %d\n", record.KeySize)
		fmt.Printf("ValueSize: %d\n", record.ValueSize)
		fmt.Printf("Key: %s\n", record.Key)
		fmt.Printf("Value: %s\n", record.Value)
	}
	return nil
}

// Size in bytes of single record
func (record *LogRecord) recordSize() uint64 {
	var sum uint64
	sum += 4                // CRC
	sum += 16               // Timestamp
	sum += 1                // Tombstone
	sum += 8 + 8            // KeySize + ValueSize
	sum += record.KeySize   // Key
	sum += record.ValueSize // Value
	return sum
}
