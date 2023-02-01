package memtable

import (
	config "github.com/c-danil0o/NASP/Config"
	sst "github.com/c-danil0o/NASP/SSTable"
)

var Active Memtable
var Second Memtable
var Generation uint32

func Init() {
	Generation = 0
	Active = *CreateMemtable(config.MEMTABLE_CAPACITY, config.MEMTABLE_THRESHOLD, config.MEMTABLE_STRUCTURE)
	Second = *CreateMemtable(config.MEMTABLE_CAPACITY, config.MEMTABLE_THRESHOLD, config.MEMTABLE_STRUCTURE)
}
func CheckThreshold() error {
	if Active.data.Size() >= Active.Threshold {
		Second = Active
		Active.Clear()
		err := Flush(&Second)
		if err != nil {
			return err
		}
	}
	return nil
}
func Flush(mt *Memtable) error {
	list := mt.data.GetSortedData()
	//err := sst.Init(list, Generation)
	err := sst.Init(list, Generation)
	Generation++
	if err != nil {
		return err
	}
	return nil
}
