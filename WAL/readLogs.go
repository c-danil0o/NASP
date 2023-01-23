// Functions needed for reading log files
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/edsrzf/mmap-go"
)

// Reads single record from the file
func (record *LogRecord) Read(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &record.CRC); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &record.Timestamp); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &record.Tombstone); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &record.KeySize); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &record.ValueSize); err != nil {
		return err
	}
	record.Key = make([]byte, record.KeySize)
	if err := binary.Read(reader, binary.BigEndian, &record.Key); err != nil {
		return err
	}

	record.Value = make([]byte, record.ValueSize)
	if err := binary.Read(reader, binary.BigEndian, &record.Value); err != nil {
		return err
	}

	return nil
}

// Read the records from the byte slice
func readRecords(data mmap.MMap) error {
	var offset uint64
	for offset < uint64(len(data)) {
		// Create a new LogRecord
		record := &LogRecord{}

		// Read the record from the byte slice
		if err := record.Read(bytes.NewReader(data[offset:])); err != nil {
			return err
		}

		// Verify the CRC
		if record.CRC != crc32.ChecksumIEEE(record.Key) {
			return fmt.Errorf("verification")
		}

		// Print the record
		fmt.Println()
		fmt.Printf("CRC: %d\n", record.CRC)
		fmt.Printf("Timestamp: %d\n", record.Timestamp)
		fmt.Printf("Tombstone: %d\n", record.Tombstone)
		fmt.Printf("KeySize: %d\n", record.KeySize)
		fmt.Printf("ValueSize: %d\n", record.ValueSize)
		fmt.Printf("Key: %s\n", record.Key)
		fmt.Printf("Value: %s\n", record.Value)

		// Update the offset
		offset += record.recordSize()
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
