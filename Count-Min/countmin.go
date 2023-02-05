package countmin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	bloomHash "github.com/c-danil0o/NASP/BloomFilter"
	"github.com/edsrzf/mmap-go"
)

type CMS struct {
	key   [16]byte
	m     uint32
	k     uint32
	table [][]int32
	hashs []bloomHash.HashWithSeed
	seeds [][]byte
}

func createCMS(key [16]byte, epsilon float64, delta float64) *CMS {
	m_moje := CalculateM(epsilon)
	k_moje := CalculateK(delta)

	matrix := make([][]int32, k_moje)
	for i := 0; i < int(k_moje); i++ {
		matrix[i] = make([]int32, m_moje)
	}
	hashs, seeds := bloomHash.CreateHashFunctions(k_moje)
	return &CMS{
		key:   key,
		m:     m_moje,
		k:     k_moje,
		table: matrix,
		hashs: hashs,
		seeds: seeds,
	}
}

func CmsMeni() {
	file, _ := os.OpenFile("cms.bin", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	file.Close()
	for {
		fmt.Println("\n1. Create new CMS")
		fmt.Println("2. Add element to CMS")
		fmt.Println("3. Check for element in CMS")
		fmt.Println("0. Izlaz")
		fmt.Println("Select an option:")
		var choice int
		n, err := fmt.Scanf("%d\n", &choice)
		if n != 1 || err != nil {
			continue
		}

		switch choice {
		case 0:
			fmt.Println("Izlaz iz CMS menija...")
			return
		case 1:
			// Entering key and converting to [16]byte
			var aux string
			fmt.Println("Unesite kljuc za CMS: ")
			fmt.Scanf("%s\n", &aux)

			l := 16
			if len(aux) < l {
				l = len(aux)
			}

			key := [16]byte{}
			for i := 0; i < l; i++ {
				key[i] = aux[i]
			}

			cms := createCMS(key, 0.1, 0.9)
			found, _ := cms.KeyExists()
			if found {
				fmt.Println("Vec postoji CMS sa tim kljucem.")
			} else {
				// Writing into file
				if err := cms.Serialize(); err == nil {
					fmt.Println("Uspesno ste kreirali CMS.")
				} else {
					fmt.Print(err)
				}
			}
		case 2:
			// Entering key and converting to [16]byte
			var aux string
			fmt.Println("Unesite kljuc CMSa: ")
			fmt.Scanf("%s\n", &aux)

			l := 16
			if len(aux) < l {
				l = len(aux)
			}

			key := [16]byte{}
			for i := 0; i < l; i++ {
				key[i] = aux[i]
			}

			c := CMS{key: key}
			ok, pos := c.KeyExists()
			if !ok {
				fmt.Println("Ne postoji CMS sa ovim kljucem.")
			} else {
				var val string
				fmt.Println("Unesite vrednost koju zelite da ubacite u CMS: ")
				fmt.Scanf("%s\n", &val)
				c.add([]byte(val), pos)
			}
		case 3:
			// Entering key and converting to [16]byte
			var aux string
			fmt.Println("Unesite kljuc CMSa: ")
			fmt.Scanf("%s\n", &aux)

			l := 16
			if len(aux) < l {
				l = len(aux)
			}

			key := [16]byte{}
			for i := 0; i < l; i++ {
				key[i] = aux[i]
			}

			c := CMS{key: key}
			ok, _ := c.KeyExists()
			if !ok {
				fmt.Println("Ne postoji CMS sa ovim kljucem.")
			} else {
				var val string
				fmt.Println("Unesite vrednost koju zelite da proverite: ")
				fmt.Scanln(&val)
				fmt.Println("Pojavljuje se:", c.get([]byte(val)), "puta.")
				//TODO: sta sa rez?
			}
		default:
			fmt.Println("Neispravan unos. Pokusajte ponovo")
		}
	}
}

func (cms *CMS) add(value []byte, offset uint64) bool {
	for i := 0; i < len(cms.hashs); i++ {
		hashh := cms.hashs[i]
		hashed := hashh.Hash(value)
		index := hashed % uint64(cms.m)
		cms.table[i][index] += 1
	}

	// Openning file
	file, err := os.OpenFile("cms.bin", os.O_RDWR, 0600)
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
	cms.m = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	cms.k = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	for i := 0; i < int(cms.k); i++ {
		for j := 0; j < int(cms.m); j++ {
			binary.BigEndian.PutUint32(data[offset:offset+4], uint32(cms.table[i][j]))
			offset += 4
		}
	}

	// Flushing into file
	data.Flush()
	return true
}

// Returns cardinality of certain element
func (cms *CMS) get(value []byte) int32 {
	niz := make([]int32, cms.k)

	for i := 0; i < len(cms.hashs); i++ {
		hashh := cms.hashs[i]
		hashed := hashh.Hash(value)
		index := hashed % uint64(cms.m)

		niz[i] = cms.table[i][index]
	}

	min := niz[0]
	for i := 0; i < len(niz); i++ {
		if niz[i] < min {
			min = niz[i]
		}
	}
	return min
}

// returns found? and offset where that cms starts in binary file for editing
func (cms *CMS) KeyExists() (bool, uint64) {
	file, err := os.OpenFile("cms.bin", os.O_RDONLY, 0600)
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

		cms.m = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		cms.k = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		cms.table = make([][]int32, cms.k)
		for i := 0; i < int(cms.k); i++ {
			cms.table[i] = make([]int32, cms.m)
		}
		if bytes.Compare(currKey[:], cms.key[:]) == 0 {
			// If key is wanted one we load cms
			for i := 0; i < int(cms.k); i++ {
				for j := 0; j < int(cms.m); j++ {
					cms.table[i][j] = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
					offset += 4
				}
			}

			cms.seeds = make([][]byte, cms.k)
			for i := 0; i < int(cms.k); i++ {
				cms.seeds[i] = make([]byte, 32)
			}

			for i := 0; i < int(cms.k); i++ {
				copy(cms.seeds[i], data[offset:offset+32])
				offset += 4
			}

			cms.hashs = bloomHash.CreateHashFunctionsFromSeeds(cms.k, cms.seeds)

			return true, aux
		} else {
			// If key isnt wanted one we dont need to read that cms
			offset = offset + uint64(cms.k)*uint64(cms.m)*4 + uint64(cms.k)*32
		}
	}
	return false, 0
}

func (cms *CMS) Serialize() error {
	file, err := os.OpenFile("cms.bin", os.O_WRONLY|os.O_APPEND, 0600)
	defer file.Close()
	if err != nil {
		return err
	}

	// Writing record
	if err := cms.Write(file); err != nil {
		return err
	}

	// Flushing file
	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

// Writing single key-cms record
func (cms *CMS) Write(writer io.Writer) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, cms.key)
	binary.Write(&buf, binary.BigEndian, cms.m)
	binary.Write(&buf, binary.BigEndian, cms.k)
	for i := 0; i < int(cms.k); i++ {
		binary.Write(&buf, binary.BigEndian, cms.table[i])
	}
	for i := 0; i < int(cms.k); i++ {
		binary.Write(&buf, binary.BigEndian, cms.seeds[i])
	}

	_, err := writer.Write(buf.Bytes())
	return err
}
