package bloomfilter

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type BloomFilter struct {
	max_size int
	bits     []bool
	m        uint32
	k        uint32
	n        uint
	hashfns  []HashWithSeed
	seeds    [][]byte
}

func NewBloomFilter(size int, fpr float64) *BloomFilter {
	calculatedM := CalculateM(size, fpr)
	calculatedK := CalculateK(size, calculatedM)
	hshfns, seeds := CreateHashFunctions(calculatedK)
	return &BloomFilter{
		max_size: size,
		bits:     make([]bool, calculatedM),
		n:        uint(0),
		k:        calculatedK,
		m:        calculatedM,
		hashfns:  hshfns,
		seeds:    seeds,
	}
}

func (bf *BloomFilter) Add(value []byte) {
	for _, i := range bf.hashfns {
		hashedValue := i.Hash(value)
		position := uint32(hashedValue) % bf.m
		bf.bits[position] = true

	}
	bf.n += 1
}

func (bf *BloomFilter) Find(value []byte) bool {
	for _, i := range bf.hashfns {
		hashedValue := i.Hash(value)
		position := uint32(hashedValue) % bf.m
		if !bf.bits[position] {
			return false
		}

	}
	return true

}

// serializing bits, m, k, seeds
func (bf *BloomFilter) Serialize(writer io.Writer) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, bf.m)
	err = binary.Write(&buf, binary.BigEndian, bf.k)
	err = binary.Write(&buf, binary.BigEndian, bf.bits)
	for i := 0; i < int(bf.k); i++ {
		err = binary.Write(&buf, binary.BigEndian, bf.seeds[i])
	}
	if err != nil {
		return err
	}
	_, err = writer.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func Read(file *os.File, offset int64) (*BloomFilter, error) {
	bf := BloomFilter{}
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &bf.m); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &bf.k); err != nil {
		return nil, err
	}
	bf.bits = make([]bool, bf.m)
	bf.seeds = make([][]byte, bf.k)
	for i := 0; i < int(bf.k); i++ {
		bf.seeds[i] = make([]byte, 32)
	}
	if err := binary.Read(file, binary.BigEndian, &bf.bits); err != nil {
		return nil, err
	}

	for i := 0; i < int(bf.k); i++ {
		if err := binary.Read(file, binary.BigEndian, &bf.seeds[i]); err != nil {
			return nil, err
		}
	}

	bf.hashfns = CreateHashFunctionsFromSeeds(bf.k, bf.seeds)
	return &bf, nil
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
