package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Summary struct {
	keys        [][]byte
	positions   []uint64
	first       []byte
	last        []byte
	summarySize int32
}

func NewSummary(summarySize int32) *Summary {
	keys := make([][]byte, summarySize)
	for i := range keys {
		keys[i] = make([]byte, 0)
	}
	positions := make([]uint64, summarySize)
	return &Summary{
		keys:        keys,
		positions:   positions,
		summarySize: summarySize,
	}
}

func (summary *Summary) WriteSummary(writer io.Writer, last []byte) (int64, error) {
	var size int64
	var buf bytes.Buffer
	summary.first = summary.keys[0]
	summary.last = last
	firstSize := uint64(len(summary.first))
	lastSize := uint64(len(summary.last))

	err := binary.Write(&buf, binary.BigEndian, firstSize)
	if err != nil {
		return 0, err
	}
	err = binary.Write(&buf, binary.BigEndian, lastSize)
	if err != nil {
		return 0, err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.first)
	if err != nil {
		return 0, err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.last)
	if err != nil {
		return 0, err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.summarySize)
	if err != nil {
		return 0, err
	}
	_, err = writer.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	size = int64(len(buf.Bytes()))
	buf.Reset()

	for i := 0; i < int(summary.summarySize); i++ {
		err := binary.Write(&buf, binary.BigEndian, uint64(len(summary.keys[i])))
		err = binary.Write(&buf, binary.BigEndian, summary.keys[i])
		if err != nil {
			return 0, err
		}
		err = binary.Write(&buf, binary.BigEndian, summary.positions[i])
		if err != nil {
			return 0, err
		}
		_, err = writer.Write(buf.Bytes())
		if err != nil {
			return 0, err
		}
		size += int64(len(buf.Bytes()))
		buf.Reset()
	}
	return size, nil
}

func ReadFirstLast(file *os.File, offset int64) ([]byte, []byte, error) {
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, nil, err
	}
	var firstSize, lastSize uint64
	if err := binary.Read(file, binary.BigEndian, &firstSize); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &lastSize); err != nil {
		return nil, nil, err
	}
	first := make([]byte, firstSize)
	last := make([]byte, lastSize)
	if err := binary.Read(file, binary.BigEndian, &first); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &last); err != nil {
		return nil, nil, err
	}
	return first, last, err
}

func ReadSummary(file *os.File, offset int64) (*Summary, error) {
	summ := Summary{}
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	var firstSize, lastSize uint64
	if err := binary.Read(file, binary.BigEndian, &firstSize); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &lastSize); err != nil {
		return nil, err
	}
	summ.first = make([]byte, firstSize)
	summ.last = make([]byte, lastSize)
	if err := binary.Read(file, binary.BigEndian, &summ.first); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &summ.last); err != nil {
		return nil, err
	}

	if err := binary.Read(file, binary.BigEndian, &summ.summarySize); err != nil {
		return nil, err
	}
	summ.keys = make([][]byte, summ.summarySize)
	summ.positions = make([]uint64, summ.summarySize)
	for i := 0; i < int(summ.summarySize); i++ {
		var keysize uint64
		if err := binary.Read(file, binary.BigEndian, &keysize); err != nil {
			return nil, err
		}
		summ.keys[i] = make([]byte, keysize)
		err := binary.Read(file, binary.BigEndian, &summ.keys[i])
		if err != nil {
			return nil, err
		}
		err = binary.Read(file, binary.BigEndian, &summ.positions[i])
		if err != nil {
			return nil, err

		}
	}
	return &summ, nil
}

func (summary *Summary) FindKey(key []byte) (int64, error) {
	for i := 0; i < int(summary.summarySize); i++ {
		if bytes.Compare(summary.keys[i], key) == 0 {
			return int64(summary.positions[i]), nil
		} else if i == int(summary.summarySize)-1 {
			return int64(summary.positions[i]), nil
		} else if bytes.Compare(summary.keys[i], key) == -1 && bytes.Compare(summary.keys[i+1], key) == 1 {
			return int64(summary.positions[i]), nil
		}
	}
	return 0, fmt.Errorf("not found")
}

func (summary *Summary) FindPrefixKeys(key []byte) []int64 {
	var retVal []int64
	for i := 0; i < int(summary.summarySize); i++ {
		if bytes.HasPrefix(summary.keys[i], key) {
			retVal = append(retVal, int64(summary.positions[i]))
		}
	}
	return retVal
}

func (summary *Summary) FindRangeKeys(min []byte, max []byte) []int64 {
	var retVal []int64
	for i := 0; i < int(summary.summarySize); i++ {
		if bytes.Compare(max, summary.keys[i]) >= 0 && bytes.Compare(min, summary.keys[i]) <= 0 {
			retVal = append(retVal, int64(summary.positions[i]))
		}
	}
	return retVal
}

func (summary *Summary) UpdateOffset(offset uint64) {
	for i := 0; i < int(summary.summarySize); i++ {
		summary.positions[i] += offset
	}
}
