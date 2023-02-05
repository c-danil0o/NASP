package hyperloglog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

type hyperLogLog struct {
	key       [16]byte
	b         uint32   //bits precision
	m         uint32   //set size
	registers []uint32 //
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

func newHyperLogLog(key [16]byte, b uint32) *hyperLogLog {
	m := uint32(math.Pow(2.0, float64(b)))
	return &hyperLogLog{
		key:       key,
		b:         b,
		m:         m,
		registers: make([]uint32, m),
	}
}

func (hll *hyperLogLog) add(value []byte, offset uint64) bool {
	hashedValue := generateHash(value)
	zeros := uint32(findTrailingZeros(hashedValue))
	bucket := uint32(hashedValue) >> uint32(32-hll.b)
	if hll.registers[bucket] < zeros {
		hll.registers[bucket] = zeros
	}

	// Openning file
	file, err := os.OpenFile("hll.bin", os.O_RDWR, 0600)
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return false
	}

	fi, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return false
	}

	if fi.Size() == 0 {
		return false
	}

	// Memory map the file
	data, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer data.Unmap()

	offset += 16 //skipping key
	hll.b = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	hll.m = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	for i := 0; i < int(hll.m); i++ {
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(hll.registers[i]))
		offset += 4
	}

	// Flushing into file
	data.Flush()
	return true
}

func (hll *hyperLogLog) estimate() float64 {
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

func (hll *hyperLogLog) serialize() error {
	file, err := os.OpenFile("hll.bin", os.O_WRONLY|os.O_APPEND, 0600)
	defer file.Close()
	if err != nil {
		return err
	}

	// Writing record
	if err := hll.write(file); err != nil {
		return err
	}

	// Flushing file
	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func (hll *hyperLogLog) write(writer io.Writer) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, hll.key)
	err = binary.Write(&buf, binary.BigEndian, hll.b)
	err = binary.Write(&buf, binary.BigEndian, hll.m)
	for _, reg := range hll.registers {
		err = binary.Write(&buf, binary.BigEndian, reg)
	}
	_, err = writer.Write(buf.Bytes())
	return err
}

// returns found? and offset where that cms starts in binary file for editing
func (hll *hyperLogLog) keyExists() (bool, uint64) {
	file, err := os.OpenFile("hll.bin", os.O_RDONLY, 0600)
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return false, 0
	}

	fi, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return false, 0
	}

	if fi.Size() == 0 {
		return false, 0
	}
	// Memory map the file
	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		fmt.Println(err)
		return false, 0
	}
	defer data.Unmap()

	var offset uint64
	var currKey [16]byte
	for offset < uint64(len(data)) {
		aux := offset
		copy(currKey[:], data[offset:offset+16])
		offset += 16

		hll.b = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		hll.m = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		if bytes.Compare(currKey[:], hll.key[:]) == 0 {
			// If key is wanted one we load cms
			hll.registers = make([]uint32, hll.m)
			for i := 0; i < int(hll.m); i++ {
				hll.registers[i] = binary.BigEndian.Uint32(data[offset : offset+4])
				offset += 4
			}

			return true, aux
		} else {
			// If key isnt wanted one we dont need to read that cms
			offset = offset + uint64(hll.m)*4
		}
	}
	return false, 0
}

func Menu() {
	// Creating and truncating file
	file, _ := os.OpenFile("hll.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	file.Close()
	for {
		fmt.Println("\n1. Kreiranje novog HLL")
		fmt.Println("2. Dodavanje elementa u HLL")
		fmt.Println("3. Provjera kardinalnosti u HLL")
		fmt.Println("0. Izlaz")
		fmt.Println("Opcija:")
		var choice int
		n, err := fmt.Scanf("%d\n", &choice)
		if n != 1 || err != nil {
			continue
		}

		switch choice {
		case 0:
			fmt.Println("Izlaz iz HLL menija...")
			return
		case 1:
			// Entering key and converting to [16]byte
			var aux string
			fmt.Println("Unesite kljuc za novi HLL: ")
			fmt.Scanf("%s\n", &aux)

			l := 16
			if len(aux) < l {
				l = len(aux)
			}

			key := [16]byte{}
			for i := 0; i < l; i++ {
				key[i] = aux[i]
			}

			h := newHyperLogLog(key, 16)
			found, _ := h.keyExists()
			if found {
				fmt.Println("Vec postoji HLL sa tim kljucem.")
			} else {
				// Writing into file
				if err := h.serialize(); err == nil {
					fmt.Println("Uspesno ste kreirali HLL.")
				} else {
					fmt.Print(err)
				}
			}
		case 2:
			// Entering key and converting to [16]byte
			var aux string
			fmt.Println("Unesite kljuc HLL-a: ")
			fmt.Scanf("%s\n", &aux)

			l := 16
			if len(aux) < l {
				l = len(aux)
			}

			key := [16]byte{}
			for i := 0; i < l; i++ {
				key[i] = aux[i]
			}

			c := newHyperLogLog(key, 16)
			ok, pos := c.keyExists()
			if !ok {
				fmt.Println("Ne postoji HLL sa ovim kljucem.")
			} else {
				var val string
				fmt.Println("Unesite vrednost koju zelite da ubacite u HLL: ")
				fmt.Scanf("%s\n", &val)
				c.add([]byte(val), pos)
			}
		case 3:
			// Entering key and converting to [16]byte
			var aux string
			fmt.Println("Unesite kljuc HLL: ")
			fmt.Scanf("%s\n", &aux)

			l := 16
			if len(aux) < l {
				l = len(aux)
			}

			key := [16]byte{}
			for i := 0; i < l; i++ {
				key[i] = aux[i]
			}

			c := newHyperLogLog(key, 16)
			ok, _ := c.keyExists()
			if !ok {
				fmt.Println("Ne postoji HLL sa ovim kljucem.")
			} else {
				fmt.Println("Unikatnih elemenata u ovom HLL je: ", c.estimate())
			}
		default:
			fmt.Println("Neispravan unos. Pokusajte ponovo")
		}
	}
}
