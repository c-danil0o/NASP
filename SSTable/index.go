package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	//skiplist "github.com/c-danil0o/NASP/SkipList"
	"io"
)

type Index struct {
	keys      [][]byte
	positions []uint64
	size      uint
}

func NewIndex(sstable *SSTable) *Index {
	keys := make([][]byte, sstable.DataSize)
	for i := range keys {
		keys[i] = make([]byte, 0)
	}
	positions := make([]uint64, sstable.DataSize)
	return &Index{
		keys:      keys,
		positions: positions,
		size:      uint(sstable.DataSize),
	}
}
func (index *Index) indexSize() uint {
	var count uint = 0
	for int(count) < len(index.keys) && len(index.keys[count]) != 0 {
		count++
	}
	return count
}

func (index *Index) WriteIndex(writer io.Writer) error {
	var buf bytes.Buffer
	for i := 0; i < int(index.indexSize()); i++ {
		err := binary.Write(&buf, binary.BigEndian, uint64(len(index.keys[i])))
		if err != nil {
			return err
		}
		err = binary.Write(&buf, binary.BigEndian, index.keys[i])
		if err != nil {
			return err
		}
		err = binary.Write(&buf, binary.BigEndian, index.positions[i])
		if err != nil {
			return err
		}
		_, err = writer.Write(buf.Bytes())
		if err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

func FindIDSegment(key []byte, file *os.File, start int64, count int) (int64, error) {
	_, err := file.Seek(start, 0)
	if err != nil {
		return 0, err
	}
	for i := 0; i < count; i++ {
		var keylen uint64
		if err := binary.Read(file, binary.BigEndian, &keylen); err != nil {
			return 0, err
		}
		indexKey := make([]byte, keylen)
		err := binary.Read(file, binary.BigEndian, &indexKey)
		if err != nil {
			return 0, err
		}
		var position uint64
		if err := binary.Read(file, binary.BigEndian, &position); err != nil {
			return 0, err
		}
		if bytes.Compare(indexKey, key) == 0 {
			return int64(position), nil
		}
	}
	return 0, fmt.Errorf("not found")
}

func FindIDSegments(key []byte, file *os.File, start int64, stop int64) ([]int64, error) {
	_, err := file.Seek(start, 0)
	if err != nil {
		return nil, err
	}
	var retVal []int64
	var startoff = start
	for startoff <= stop {
		var keylen uint64
		if err := binary.Read(file, binary.BigEndian, &keylen); err != nil {
			return nil, err
		}
		indexKey := make([]byte, keylen)
		err := binary.Read(file, binary.BigEndian, &indexKey)
		if err != nil {
			return nil, err
		}
		var position uint64
		if err := binary.Read(file, binary.BigEndian, &position); err != nil {
			return nil, err
		}
		if bytes.HasPrefix(indexKey, key) {
			retVal = append(retVal, int64(position))
		}
		startoff += 8 + 8 + int64(keylen)
	}
	return retVal, nil
}

func FindIDSegmentsMultiple(key []byte, file *os.File, start int64) ([]int64, error) {
	_, err := file.Seek(start, 0)
	if err != nil {
		return nil, err
	}
	var retVal []int64
	for true {
		var keylen uint64
		if err := binary.Read(file, binary.BigEndian, &keylen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		indexKey := make([]byte, keylen)
		err := binary.Read(file, binary.BigEndian, &indexKey)
		if err != nil {
			return nil, err
		}
		var position uint64
		if err := binary.Read(file, binary.BigEndian, &position); err != nil {
			return nil, err
		}
		if bytes.HasPrefix(indexKey, key) {
			retVal = append(retVal, int64(position))
		}
	}
	return retVal, nil
}

func FindRangeIDSegments(minKey []byte, maxKey []byte, file *os.File, start int64, count int) ([]int64, error) {
	_, err := file.Seek(start, 0)
	if err != nil {
		return nil, err
	}
	var retVal []int64
	for i := 0; i < count; i++ {
		var keylen uint64
		if err := binary.Read(file, binary.BigEndian, &keylen); err != nil {
			return nil, err
		}
		indexKey := make([]byte, keylen)
		err := binary.Read(file, binary.BigEndian, &indexKey)
		if err != nil {
			return nil, err
		}
		var position uint64
		if err := binary.Read(file, binary.BigEndian, &position); err != nil {
			return nil, err
		}
		if bytes.Compare(indexKey, minKey) >= 0 && bytes.Compare(maxKey, indexKey) >= 0 {
			retVal = append(retVal, int64(position))
		}
	}
	return retVal, nil
}
