package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// Record implements DataNode interface
type Record struct {
	CRC       uint32
	timestamp int64
	tombstone byte
	KeySize   uint64
	ValueSize uint64
	key       []byte
	value     []byte
}

func (rec *Record) Value() []byte {
	return rec.value
}
func (rec *Record) Key() []byte {
	return rec.key
}
func (rec *Record) Tombstone() byte {
	return rec.tombstone
}
func (rec *Record) Timestamp() int64 {
	//return int64(binary.BigEndian.Uint64(rec.timestamp[:]))
	return rec.timestamp
}
func (rec *Record) SetKey(key []byte) {
	rec.key = key
}
func (rec *Record) SetValue(value []byte) {
	rec.value = value
}
func (rec *Record) SetTimestamp(timestamp int64) {
	//binary.PutVarint(rec.timestamp[:], timestamp)
	rec.timestamp = timestamp
}
func (rec *Record) SetTombstone(tombstone byte) {
	rec.tombstone = tombstone
}

func (rec *Record) RecordSize() uint64 {
	var sum uint64
	sum += 4             // CRC
	sum += 8             // Timestamp
	sum += 1             // Tombstone
	sum += 8 + 8         // KeySize + ValueSize
	sum += rec.KeySize   // Key
	sum += rec.ValueSize // Value
	return sum
}

func (rec *Record) Write(writer io.Writer) error {
	// Making CRC hash
	rec.CRC = crc32.ChecksumIEEE(rec.key)

	// Getting current time
	rec.timestamp = time.Now().UnixNano()
	//binary.PutVarint(rec.timestamp[:], timestamp)

	// Getting length of key
	keyLen := uint64(len(rec.key))
	rec.KeySize = keyLen

	// Getting length of value
	valueLen := uint64(len(rec.value))
	rec.ValueSize = valueLen

	// Create a buffer for the rec
	var buf bytes.Buffer

	// Write the rec to the buffer
	binary.Write(&buf, binary.BigEndian, rec.CRC)
	binary.Write(&buf, binary.BigEndian, rec.timestamp)
	binary.Write(&buf, binary.BigEndian, rec.tombstone)
	binary.Write(&buf, binary.BigEndian, rec.KeySize)
	binary.Write(&buf, binary.BigEndian, rec.ValueSize)
	binary.Write(&buf, binary.BigEndian, rec.key)
	binary.Write(&buf, binary.BigEndian, rec.value)

	// Write the buffer to the file
	_, err := writer.Write(buf.Bytes())

	return err
}
func (rec *Record) Read(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &rec.CRC); err != nil {
		return err
	}

	if err := binary.Read(reader, binary.BigEndian, &rec.timestamp); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.tombstone); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.KeySize); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &rec.ValueSize); err != nil {
		return err
	}

	rec.key = make([]byte, rec.KeySize)
	if err := binary.Read(reader, binary.BigEndian, &rec.key); err != nil {
		return err
	}

	rec.value = make([]byte, rec.ValueSize)
	if err := binary.Read(reader, binary.BigEndian, &rec.value); err != nil {
		return err
	}
	if rec.CRC != crc32.ChecksumIEEE(rec.key) {
		return fmt.Errorf("verification")
	}
	return nil
}

func (rec *Record) ReadNext(reader io.Reader) bool {
	err := rec.Read(reader)
	if err != nil {
		return false
	}
	for rec.Tombstone() == 1 {
		err = rec.Read(reader)
		if err != nil {
			return false
		}
	}
	return true
}

func (rec *Record) ReadNextSingle(reader *os.File, offset int64) bool {
	off, err := reader.Seek(0, 1)
	if off == offset {
		return false
	}
	reader.Seek(off, 0)

	err = rec.Read(reader)
	if err != nil {
		return false
	}
	for rec.Tombstone() == 1 {
		err = rec.Read(reader)
		if err != nil {
			return false
		}
	}
	return true
}
