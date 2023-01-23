package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

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

// Reads single record
func (record *LogRecord) Read(reader io.Reader) error {
	// Read the record from the file
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

// Main function for writing record
func writeWal(file *os.File, record LogRecord) error {
	// Write the record to the file
	if err := record.Write(file); err != nil {
		return err
	}

	// Flush the file
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

// Size in bytes of single record
func (record *LogRecord) recordSize() uint64 {
	var sum uint64
	sum += 4     // CRC
	sum += 16    // Timestamp
	sum += 1     // Tombstone
	sum += 8 + 8 // KeySize + ValueSize
	sum += record.KeySize
	sum += record.ValueSize
	return sum
}

func main() {
	// Open the WAL file (for seeing why permission is set to 0600 visit https://phoenixnap.com/kb/linux-file-permissions)
	file, err := os.OpenFile("wal.log", os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Test cases
	record1 := LogRecord{
		Tombstone: 0,
		Key:       []byte("aleksa"),
		Value:     []byte("perovic"),
	}

	record2 := LogRecord{
		Tombstone: 0,
		Key:       []byte("marko"),
		Value:     []byte("stojanovic"),
	}

	record3 := LogRecord{
		Tombstone: 1,
		Key:       []byte("danilo"),
		Value:     []byte("cvijetic"),
	}

	if err := writeWal(file, record1); err != nil {
		fmt.Println(err)
		return
	}
	if err := writeWal(file, record2); err != nil {
		fmt.Println(err)
		return
	}
	if err := writeWal(file, record3); err != nil {
		fmt.Println(err)
		return
	}

	// Memory map the file
	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer data.Unmap()

	// Read the records from the byte slice
	var offset uint64
	for offset < uint64(len(data)) {
		// Create a new LogRecord
		record := &LogRecord{}

		// Read the record from the byte slice
		if err := record.Read(bytes.NewReader(data[offset:])); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				break
			} else {
				fmt.Println(err)
				return
			}
		}
		// Verify the CRC
		if record.CRC != crc32.ChecksumIEEE(record.Key) {
			fmt.Println("CRC verification failed.")
			return
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
}
