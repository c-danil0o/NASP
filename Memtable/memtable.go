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
		//data = *b.newBtree()
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
func (mt *Memtable) Delete(key []byte) {
	mt.data.Delete(key)
}
func (mt *Memtable) Print() {
	mt.data.Print()
}
func (mt *Memtable) Clear() {
	mt.data = container.NewSkipList()
}

func (mt *Memtable) Find(key string) container.DataNode {
	res := mt.data.Find([]byte(key))
	if res != nil {
		return res
		//return container.DataNode{
		//	Timestamp: res.Timestamp(),
		//	Tombstone: res.Tombstone(),
		//	Key:       res.Key(),
		//	Value:     res.Value(),
		//}
	}
	return nil
}
