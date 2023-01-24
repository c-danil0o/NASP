package memtable

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	s "github.com/c-danil0o/NASP/SkipList"
)

type Element struct {
	key   int
	value []byte
}

type Memtable struct {
	capacity        int
	num_of_segments int
	trashold        int
	elements        s.SkipList
}

func CreateMemtable(kapacitet int, brSegmenata int, trasholdd int) *Memtable {
	return &Memtable{
		capacity:        kapacitet,
		num_of_segments: brSegmenata,
		trashold:        trasholdd,
		elements:        *s.NewSkipList(),
	}
}

func (memtable *Memtable) Add(el Element) {
	memtable.elements.Insert(int(el.key), el.value)
}

func (memtable *Memtable) Print() {
	memtable.elements.Print()
}

func (memtable *Memtable) Push() {
	println("push-kurac")
	println(memtable.capacity)
	println(memtable.trashold)
}

func ReadFile(imefajla string) map[string]int {
	jsonFile, err := os.Open(imefajla)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]int
	json.Unmarshal([]byte(byteValue), &result)

	return result
}
