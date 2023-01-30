package memtable

import (
	"encoding/json"
	"fmt"
	sst "github.com/c-danil0o/NASP/SSTable"
	"io/ioutil"
	"os"
)

var Active Memtable
var Second Memtable
var Generation uint32

func Init(capacity int, numOfSegments int, threshold int) {
	Generation = 0
	Active = *CreateMemtable(capacity, numOfSegments, threshold)
	Second = *CreateMemtable(capacity, numOfSegments, threshold)
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
	err := sst.Init(list, Generation)
	Generation++
	if err != nil {
		return err
	}
	return nil
}
func ReadFile(filename string) map[string]int {
	jsonFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]int
	json.Unmarshal([]byte(byteValue), &result)

	return result
}
