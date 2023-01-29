package SSTable

import (
	"bytes"
	"encoding/binary"
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

func (summary *Summary) WriteSummary(writer io.Writer, last []byte) error {
	var buf bytes.Buffer
	summary.first = summary.keys[0]
	summary.last = last
	firstSize := uint64(len(summary.first))
	lastSize := uint64(len(summary.last))

	err := binary.Write(&buf, binary.BigEndian, firstSize)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.BigEndian, lastSize)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.first)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.last)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.summarySize)
	if err != nil {
		return nil
	}
	_, err = writer.Write(buf.Bytes())
	if err != nil {
		return err
	}
	buf.Reset()

	for i := 0; i < int(summary.summarySize); i++ {
		err := binary.Write(&buf, binary.BigEndian, uint64(len(summary.keys[i])))
		err = binary.Write(&buf, binary.BigEndian, summary.keys[i])
		if err != nil {
			return err
		}
		err = binary.Write(&buf, binary.BigEndian, summary.positions[i])
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

func ReadFirstLast(file *os.File, offset int64) (error, []byte, []byte) {
	_, err := file.Seek(offset, 0)
	if err != nil {
		return err, nil, nil
	}
	var firstSize, lastSize uint64
	if err := binary.Read(file, binary.BigEndian, &firstSize); err != nil {
		return err, nil, nil
	}
	if err := binary.Read(file, binary.BigEndian, &lastSize); err != nil {
		return err, nil, nil
	}
	first := make([]byte, firstSize)
	last := make([]byte, lastSize)
	if err := binary.Read(file, binary.BigEndian, &first); err != nil {
		return err, nil, nil
	}
	if err := binary.Read(file, binary.BigEndian, &last); err != nil {
		return err, nil, nil
	}
	return nil, first, last
}

func ReadSummary(file *os.File, offset int64) (error, *Summary) {
	summ := Summary{}
	_, err := file.Seek(offset, 0)
	if err != nil {
		return err, nil
	}
	var firstSize, lastSize uint64
	if err := binary.Read(file, binary.BigEndian, &firstSize); err != nil {
		return err, nil
	}
	if err := binary.Read(file, binary.BigEndian, &lastSize); err != nil {
		return err, nil
	}
	summ.first = make([]byte, firstSize)
	summ.last = make([]byte, lastSize)
	if err := binary.Read(file, binary.BigEndian, &summ.first); err != nil {
		return err, nil
	}
	if err := binary.Read(file, binary.BigEndian, &summ.last); err != nil {
		return err, nil
	}

	if err := binary.Read(file, binary.BigEndian, &summ.summarySize); err != nil {
		return err, nil
	}
	summ.keys = make([][]byte, summ.summarySize)
	summ.positions = make([]uint64, summ.summarySize)
	for i := 0; i < int(summ.summarySize); i++ {
		var keysize uint64
		if err := binary.Read(file, binary.BigEndian, &keysize); err != nil {
			return err, nil
		}
		summ.keys[i] = make([]byte, keysize)
		err := binary.Read(file, binary.BigEndian, &summ.keys[i])
		if err != nil {
			return err, nil
		}
		err = binary.Read(file, binary.BigEndian, &summ.positions[i])
		if err != nil {
			return err, nil
		}
	}

	return nil, &summ
}
