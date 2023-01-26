package SSTable

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Summary struct {
	keys        [][]byte
	positions   []uint64
	first       []byte
	last        []byte
	summarySize int
}

func NewSummary(summarySize int) *Summary {
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

func (summary *Summary) WriteSummary(writer io.Writer) error {
	var buf bytes.Buffer
	summary.first = summary.keys[0]
	summary.last = summary.keys[summary.summarySize-1]

	err := binary.Write(&buf, binary.BigEndian, summary.first)
	if err != nil {
		return err
	}
	err = binary.Write(&buf, binary.BigEndian, summary.last)
	if err != nil {
		return err
	}
	_, err = writer.Write(buf.Bytes())
	if err != nil {
		return err
	}
	buf.Reset()

	for i := 0; i < int(summary.summarySize); i++ {
		err := binary.Write(&buf, binary.BigEndian, summary.keys[i])
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
