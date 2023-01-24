package memtable

import s "github.com/c-danil0o/NASP/SkipList"

type Element struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone byte
}

type Memtable struct {
	capacity      int
	numOfSegments int
	threshold     int
	data          s.SkipList
}

func CreateMemtable(capacity int, numOfSegments int, threshold int) *Memtable {
	return &Memtable{
		capacity:      capacity,
		numOfSegments: numOfSegments,
		threshold:     threshold,
		data:          *s.NewSkipList(),
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
	mt.data = *s.NewSkipList()
}
