package countmin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	bloomHash "github.com/c-danil0o/NASP/BloomFilter"
)

type CMS struct {
	m     uint32
	k     uint32
	table [][]int32
	hashs []bloomHash.HashWithSeed
	seeds [][]byte
}

func createCMS(epsilon float64, delta float64) *CMS {
	m_moje := CalculateM(epsilon)
	k_moje := CalculateK(delta)

	matrix := make([][]int32, k_moje)
	for i := 0; i < int(k_moje); i++ {
		matrix[i] = make([]int32, m_moje)
	}
	hashs, seeds := bloomHash.CreateHashFunctions(k_moje)
	return &CMS{
		m:     m_moje,
		k:     k_moje,
		table: matrix,
		hashs: hashs,
		seeds: seeds,
	}
}

var CMSs map[string]*CMS

func CmsMeni() {
	for {
		fmt.Println("Select an option:")
		fmt.Println("1. Create new CMS")
		fmt.Println("2. Add element to CMS")
		fmt.Println("3. Check for element in CMS")
		fmt.Println("0. Izlaz")

		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 0:
			fmt.Println("Izlaz iz CMS menija...")
			os.Exit(0)
		case 1:
			var key string
			fmt.Println("Unesite kljuc za CMS: ")
			fmt.Scanln(&key)
			_, ok := CMSs[key]
			if ok {
				fmt.Println("Vec postoji CMS sa tim kljucem.")
			} else {
				CMSs[key] = createCMS(0.1, 0.9) //DODATI PARAMETRE U CONFIG
				fmt.Println("Uspesno ste kreirali CMS.")
				//OVO SERIJALIZOVATI
				//TODO
			}
		case 2:
			var key string
			fmt.Println("Unesite kljuc CMSa: ")
			fmt.Scanln(&key)
			//TODO
			//DESERIJALIZUJ CMS KAD NADJES KLJUC
			//stavi da ti je c taj deserijalizovan

			c, ok := CMSs[key]

			if !ok {
				fmt.Println("Ne postoji CMS sa ovim kljucem.")
			} else {
				var val string
				fmt.Println("Unesite vrednost koju zelite da ubacite u CMS: ")
				fmt.Scanln(&val)

				c.add([]byte(val))

			}
		case 3:
			var key string
			fmt.Println("Unesite kljuc CMSa: ")
			fmt.Scanln(&key)
			//TODO
			//DESERIJALIZUJ CMS KAD NADJES KLJUC

			c, ok := CMSs[key]

			if !ok {
				fmt.Println("Ne postoji CMS sa ovim kljucem.")
			} else {
				var val string
				fmt.Println("Unesite vrednost koju zelite da proverite: ")
				fmt.Scanln(&val)
				fmt.Println("Pojavljuje se: " + string(c.get([]byte(val))) + " puta.")
				//TODO
				//PROVERITI SA CELAVIM STA SA REZULTATIMA
			}
		}
	}
}

func (cms *CMS) add(value []byte) {
	for i := 0; i < len(cms.hashs); i++ {
		hashh := cms.hashs[i]
		hashed := hashh.Hash(value)
		index := hashed % uint64(cms.m)
		cms.table[i][index] += 1
	}
}

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

func (cms *CMS) Serialize(writer io.Writer) error {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, cms.m)
	err = binary.Write(&buf, binary.BigEndian, cms.k)
	for i := 0; i < int(cms.k); i++ {
		err = binary.Write(&buf, binary.BigEndian, cms.table[i])
	}
	for i := 0; i < int(cms.k); i++ {
		err = binary.Write(&buf, binary.BigEndian, cms.seeds[i])
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

func Read(file *os.File) (*CMS, error) {
	cms := CMS{}
	if err := binary.Read(file, binary.BigEndian, &cms.m); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.BigEndian, &cms.k); err != nil {
		return nil, err
	}

	cms.table = make([][]int32, cms.k)
	for i := 0; i < int(cms.k); i++ {
		cms.table[i] = make([]int32, cms.m)
	}
	cms.seeds = make([][]byte, cms.k)
	for i := 0; i < int(cms.k); i++ {
		cms.seeds[i] = make([]byte, 32)
	}
	for i := 0; i < int(cms.k); i++ {
		for j := 0; j < int(cms.m); j++ {
			if err := binary.Read(file, binary.BigEndian, &cms.table[i][j]); err != nil {
				return nil, err
			}
		}
	}
	for i := 0; i < int(cms.k); i++ {
		if err := binary.Read(file, binary.BigEndian, &cms.seeds[i]); err != nil {
			return nil, err
		}
	}

	cms.hashs = bloomHash.CreateHashFunctionsFromSeeds(cms.k, cms.seeds)

	return &cms, nil
}
