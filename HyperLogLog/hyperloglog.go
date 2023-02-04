package hyperloglog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"hash/fnv"
	"io"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"time"
)

type hyperLogLog struct {
	b         uint   //bits precision
	m         uint   //set size
	registers []uint //
}

func generateHash(in []byte) uint32 {
	hsh := fnv.New32()
	hsh.Write(in)
	sum := hsh.Sum32()
	hsh.Reset()
	return sum
}

func findTrailingZeros(n uint32) int {
	return 1 + bits.TrailingZeros32(n)
}

func NewHyperLogLog(b uint) *hyperLogLog {
	m := uint(math.Pow(2.0, float64(b)))
	return &hyperLogLog{
		b:         b,
		m:         m,
		registers: make([]uint, m),
	}
}

func (hll *hyperLogLog) Add(value []byte) {
	hashedValue := generateHash(value)
	zeros := uint(findTrailingZeros(hashedValue))
	bucket := uint(hashedValue) >> uint(32-hll.b)
	if hll.registers[bucket] < zeros {
		hll.registers[bucket] = zeros
	}
}

func (hll *hyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.registers {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *hyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.registers {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func GetRandomData() (out [][]byte, intout []uint32) {
	for i := 0; i < math.MaxInt16; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Uint32()
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, i)
		out = append(out, b)
		intout = append(intout, i)
	}
	return
}

func (hll *hyperLogLog) SerializeHLL(writer io.Writer) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, hll.b)
	err = binary.Write(&buf, binary.BigEndian, hll.m)
	for _, reg := range hll.registers {
		err = binary.Write(&buf, binary.BigEndian, reg)
	}
	_, err = writer.Write(buf.Bytes())
	return err
}

func DeserializeHLL(file os.File) (*hyperLogLog, error) {
	decoder := gob.NewDecoder(&file)
	var hll = new(hyperLogLog)
	_, err := file.Seek(0, 0)
	err = decoder.Decode(hll)
	if err != nil {
		//fmt.Println("Doslo je do greske.")
		return nil, err
	}
	return hll, err
}

//func main() {
//	bs, _ := getRandomData()
//	h := newHyperLogLog(16)
//	for _, b := range bs {
//		h.add(b)
//	}
//	hd := h.Estimate()
//	fmt.Println(hd)
//
//}
