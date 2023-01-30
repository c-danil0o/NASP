package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type Record struct {
	CRC       uint32
	Timestamp [16]byte
	Tombstone byte
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

func (rec *Record) RecordSize() uint64 {
	var sum uint64
	sum += 4             // CRC
	sum += 16            // Timestamp
	sum += 1             // Tombstone
	sum += 8 + 8         // KeySize + ValueSize
	sum += rec.KeySize   // Key
	sum += rec.ValueSize // Value
	return sum
}

func (rec *Record) Write(writer io.Writer) error {
	// Making CRC hash
	rec.CRC = crc32.ChecksumIEEE(rec.Key)

	// Getting current time
	timestamp := time.Now().UnixNano()
	binary.PutVarint(rec.Timestamp[:], timestamp)

	// Getting length of key
	keyLen := uint64(len(rec.Key))
	rec.KeySize = keyLen

	// Getting length of value
	valueLen := uint64(len(rec.Value))
	rec.ValueSize = valueLen

	// Create a buffer for the rec
	var buf bytes.Buffer

	// Write the rec to the buffer
	binary.Write(&buf, binary.BigEndian, rec.CRC)
	binary.Write(&buf, binary.BigEndian, rec.Timestamp)
	binary.Write(&buf, binary.BigEndian, rec.Tombstone)
	binary.Write(&buf, binary.BigEndian, rec.KeySize)
	binary.Write(&buf, binary.BigEndian, rec.ValueSize)
	binary.Write(&buf, binary.BigEndian, rec.Key)
	binary.Write(&buf, binary.BigEndian, rec.Value)

	// Write the buffer to the file
	_, err := writer.Write(buf.Bytes())

	return err
}
func (rec *Record) Read(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &rec.CRC); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.Timestamp); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.Tombstone); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.KeySize); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.ValueSize); err != nil {
		return err
	}

	rec.Key = make([]byte, rec.KeySize)
	if err := binary.Read(reader, binary.BigEndian, &rec.Key); err != nil {
		return err
	}

	rec.Value = make([]byte, rec.ValueSize)
	if err := binary.Read(reader, binary.BigEndian, &rec.Value); err != nil {
		return err
	}
	if rec.CRC != crc32.ChecksumIEEE(rec.Key) {
		return fmt.Errorf("verification")
	}
	return nil
}
