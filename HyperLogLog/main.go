package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"math/rand"
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

func newHyperLogLog(b uint) *hyperLogLog {
	m := uint(math.Pow(2.0, float64(b)))
	return &hyperLogLog{
		b:         b,
		m:         m,
		registers: make([]uint, m),
	}
}

func (hll *hyperLogLog) add(value []byte) {
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

func getRandomData() (out [][]byte, intout []uint32) {
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
func main() {
	bs, _ := getRandomData()
	h := newHyperLogLog(16)
	for _, b := range bs {
		h.add(b)
	}
	hd := h.Estimate()
	fmt.Println(hd)

}
