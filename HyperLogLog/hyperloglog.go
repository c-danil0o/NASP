// package hyperloglog

// import (
// 	"bytes"
// 	"encoding/binary"
// 	"fmt"
// 	"hash/fnv"
// 	"io"
// 	"math"
// 	"math/bits"
// 	"math/rand"
// 	"os"
// 	"time"
// )

// type hyperLogLog struct {
// 	key       [16]byte
// 	b         uint   //bits precision
// 	m         uint   //set size
// 	registers []uint //
// }

// func generateHash(in []byte) uint32 {
// 	hsh := fnv.New32()
// 	hsh.Write(in)
// 	sum := hsh.Sum32()
// 	hsh.Reset()
// 	return sum
// }

// func findTrailingZeros(n uint32) int {
// 	return 1 + bits.TrailingZeros32(n)
// }

// func newHyperLogLog(key [16]byte, b uint) *hyperLogLog {
// 	m := uint(math.Pow(2.0, float64(b)))
// 	return &hyperLogLog{
// 		key:       key,
// 		b:         b,
// 		m:         m,
// 		registers: make([]uint, m),
// 	}
// }

// func (hll *hyperLogLog) add(value []byte) {
// 	hashedValue := generateHash(value)
// 	zeros := uint(findTrailingZeros(hashedValue))
// 	bucket := uint(hashedValue) >> uint(32-hll.b)
// 	if hll.registers[bucket] < zeros {
// 		hll.registers[bucket] = zeros
// 	}
// }

// func (hll *hyperLogLog) estimate() float64 {
// 	sum := 0.0
// 	for _, val := range hll.registers {
// 		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
// 	}

// 	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
// 	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
// 	emptyRegs := hll.emptyCount()
// 	if estimation <= 2.5*float64(hll.m) { // do small range correction
// 		if emptyRegs > 0 {
// 			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
// 		}
// 	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
// 		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
// 	}
// 	return estimation
// }

// func (hll *hyperLogLog) emptyCount() int {
// 	sum := 0
// 	for _, val := range hll.registers {
// 		if val == 0 {
// 			sum++
// 		}
// 	}
// 	return sum
// }

// func getRandomData() (out [][]byte, intout []uint32) {
// 	for i := 0; i < math.MaxInt16; i++ {
// 		rand.Seed(time.Now().UnixNano())
// 		i := rand.Uint32()
// 		b := make([]byte, 4)
// 		binary.LittleEndian.PutUint32(b, i)
// 		out = append(out, b)
// 		intout = append(intout, i)
// 	}
// 	return
// }

// func (hll *hyperLogLog) serializeHLL(writer io.Writer) error {
// 	var buf bytes.Buffer
// 	err := binary.Write(&buf, binary.BigEndian, hll.b)
// 	err = binary.Write(&buf, binary.BigEndian, hll.m)
// 	for _, reg := range hll.registers {
// 		err = binary.Write(&buf, binary.BigEndian, reg)
// 	}
// 	_, err = writer.Write(buf.Bytes())
// 	return err
// }

// func Menu() {
// 	// Creating and truncating file
// 	file, _ := os.OpenFile("hll.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
// 	file.Close()
// 	for {
// 		fmt.Println("\n1. Kreiranje novog HLL")
// 		fmt.Println("2. Dodavanje elementa u HLL")
// 		fmt.Println("3. Provjera elementa u HLL")
// 		fmt.Println("0. Izlaz")
// 		fmt.Println("Opcija:")
// 		var choice int
// 		n, err := fmt.Scanf("%d\n", &choice)
// 		if n != 1 || err != nil {
// 			continue
// 		}

// 		switch choice {
// 		case 0:
// 			fmt.Println("Izlaz iz HLL menija...")
// 			return
// 		case 1:
// 			// Entering key and converting to [16]byte
// 			var aux string
// 			fmt.Println("Unesite kljuc za novi HLL: ")
// 			fmt.Scanf("%s\n", &aux)

// 			l := 16
// 			if len(aux) < l {
// 				l = len(aux)
// 			}

// 			key := [16]byte{}
// 			for i := 0; i < l; i++ {
// 				key[i] = aux[i]
// 			}

// 			// bs, _ := getRandomData()
// 			// h := newHyperLogLog(16)
// 			// for _, b := range bs {
// 			// 	h.add(b)
// 			// }
// 			// hd := h.Estimate()
// 			// fmt.Println(hd)

// 			h := newHyperLogLog(key, 0.1, 0.9)
// 			found, _ := h.KeyExists()
// 			if found {
// 				fmt.Println("Vec postoji HLL sa tim kljucem.")
// 			} else {
// 				// Writing into file
// 				if err := cms.Serialize(); err == nil {
// 					fmt.Println("Uspesno ste kreirali HLL.")
// 				} else {
// 					fmt.Print(err)
// 				}
// 			}
// 		case 2:
// 			// Entering key and converting to [16]byte
// 			var aux string
// 			fmt.Println("Unesite kljuc HLL-a: ")
// 			fmt.Scanf("%s\n", &aux)

// 			l := 16
// 			if len(aux) < l {
// 				l = len(aux)
// 			}

// 			key := [16]byte{}
// 			for i := 0; i < l; i++ {
// 				key[i] = aux[i]
// 			}

// 			c := CMS{key: key}
// 			ok, pos := c.KeyExists()
// 			if !ok {
// 				fmt.Println("Ne postoji HLL sa ovim kljucem.")
// 			} else {
// 				var val string
// 				fmt.Println("Unesite vrednost koju zelite da ubacite u HLL: ")
// 				fmt.Scanf("%s\n", &val)
// 				c.add([]byte(val), pos)
// 			}
// 		case 3:
// 			// Entering key and converting to [16]byte
// 			var aux string
// 			fmt.Println("Unesite kljuc HLL: ")
// 			fmt.Scanf("%s\n", &aux)

// 			l := 16
// 			if len(aux) < l {
// 				l = len(aux)
// 			}

// 			key := [16]byte{}
// 			for i := 0; i < l; i++ {
// 				key[i] = aux[i]
// 			}

// 			c := CMS{key: key}
// 			ok, _ := c.KeyExists()
// 			if !ok {
// 				fmt.Println("Ne postoji HLL sa ovim kljucem.")
// 			} else {
// 				var val string
// 				fmt.Println("Unesite vrednost koju zelite da proverite: ")
// 				fmt.Scanln(&val)
// 				fmt.Println("Pojavljuje se:", c.get([]byte(val)), "puta.")
// 				//TODO: sta sa rez?
// 			}
// 		default:
// 			fmt.Println("Neispravan unos. Pokusajte ponovo")
// 		}
// 	}
// }

package hyperloglog

func Menu() {

}
