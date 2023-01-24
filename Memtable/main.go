package memtable

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

var Active Memtable
var Second Memtable

func Init(capacity int, numOfSegments int, threshold int) {
	Active = *CreateMemtable(capacity, numOfSegments, threshold)
	Second = *CreateMemtable(capacity, numOfSegments, threshold)
}
func CheckThreshold() {
	if Active.data.Size() >= Active.threshold {
		Second = Active
		Active.Clear()
		Flush(&Second)
	}
}
func Flush(mt *Memtable) {
	list := mt.data.GetSortedData()
	fmt.Println(list)

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
