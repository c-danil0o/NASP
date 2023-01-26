package bloomfilter

import (
	"bytes"
	"encoding/binary"
	"io"
)

type BloomFilter struct {
	max_size int
	bits     []bool
	m        uint
	k        uint
	n        uint
	hashfns  []HashWithSeed
}

func NewBloomFilter(size int, fpr float64) *BloomFilter {
	calculatedM := CalculateM(size, fpr)
	calculatedK := CalculateK(size, calculatedM)
	return &BloomFilter{
		max_size: size,
		bits:     make([]bool, calculatedM),
		n:        uint(0),
		k:        calculatedK,
		m:        calculatedM,
		hashfns:  CreateHashFunctions(calculatedK),
	}
}

func (bf *BloomFilter) Add(value []byte) {
	for _, i := range bf.hashfns {
		hashedValue := i.Hash(value)
		position := uint(hashedValue) % bf.m
		bf.bits[position] = true

	}
	bf.n += 1
}

func (bf *BloomFilter) Find(value []byte) bool {
	for _, i := range bf.hashfns {
		hashedValue := i.Hash(value)
		position := uint(hashedValue) % bf.m
		if !bf.bits[position] {
			return false
		}

	}
	return true

}

func (bf *BloomFilter) Serialize(writer io.Writer) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, bf.bits)
	if err != nil {
		return err
	}
	_, err = writer.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (bf *BloomFilter) Deserialize() {

}

//func main() {
//	bloom := NewBloomFilter(10, 0.1)
//	bloom.add([]byte("danilo"))
//	bloom.add([]byte("golang"))
//	bloom.add([]byte("dddd"))
//
//	println(bloom.find([]byte("danilo")))
//	println(bloom.find([]byte("daniloc")))
//
//}
