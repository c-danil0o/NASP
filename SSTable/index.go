package SSTable

import (
	"bytes"
	"encoding/binary"
	//skiplist "github.com/c-danil0o/NASP/SkipList"
	"io"
)

type Index struct {
	keys      [][]byte
	keySize   uint64
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

func (index *Index) WriteIndex(writer io.Writer) error {
	var buf bytes.Buffer
	for i := 0; i < int(index.size); i++ {
		err := binary.Write(&buf, binary.BigEndian, index.keys[i])
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
