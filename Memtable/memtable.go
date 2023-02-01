package memtable

import (
	container "github.com/c-danil0o/NASP/DataContainer"
)

type Element struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone byte
}

type Memtable struct {
	capacity      int
	numOfSegments int
	Threshold     int
	data          container.Container
}

func CreateMemtable(capacity int, threshold int, structure int) *Memtable {
	var data container.Container
	if structure == 0 {
		data = container.NewSkipList()
	} else if structure == 1 {
		//data = *b.newBtree()
	}
	return &Memtable{
		capacity:  capacity,
		Threshold: threshold,
		data:      data,
	}
}

func (mt *Memtable) Add(el Element) {
	mt.data.Insert(el.Key, el.Value, el.Timestamp, el.Tombstone)

}
func (mt *Memtable) Delete(key []byte) {
	mt.data.Delete(key)
}
func (mt *Memtable) Print() {
	mt.data.Print()
}
func (mt *Memtable) Clear() {
	mt.data = container.NewSkipList()
}
