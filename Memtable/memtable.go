package memtable

import (
	"time"

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
		data = container.CreateBTree(4)
	}
	return &Memtable{
		capacity:  capacity,
		Threshold: threshold,
		data:      data,
	}
}

func (mt *Memtable) Add(key []byte, value []byte) error {
	mt.data.Insert(key, value, time.Now().UnixNano(), 0)
	return CheckThreshold()
}

func (mt *Memtable) AddDel(key []byte, value []byte) error {
	mt.data.Insert(key, value, time.Now().UnixNano(), 1)
	return CheckThreshold()
}

func (mt *Memtable) Delete(key []byte) bool {
	return mt.data.Delete(key)
}
func (mt *Memtable) Print() {
	mt.data.Print()
}
func (mt *Memtable) Clear() {
	mt.data = container.NewSkipList()
}

func (mt *Memtable) Find(key string) container.DataNode {
	return mt.data.Find([]byte(key))
}

func (mt *Memtable) PrefixScan(key string) []container.DataNode {
	return mt.data.PrefixScan([]byte(key))
}

func (mt *Memtable) RangeScan(min string, max string) []container.DataNode {
	return mt.data.RangeScan([]byte(min), []byte(max))
}
