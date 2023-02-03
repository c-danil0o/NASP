package config

import (
	"encoding/json"
	"io"
	"os"
)

// defaultne vrijednosti configa
var MEMTABLE_CAPACITY = 200
var MEMTABLE_THRESHOLD = 5
var MEMTABLE_STRUCTURE = 0 /// 0 - skip list   1 - b-tree
var SSTABLE_MULTIPLE_FILES = 0
var SSTABLE_SEGMENT_SIZE = 3
var LSM_DEPTH = 4
var CACHE_SIZE = 100
var WAL_SEGMENT_SIZE = 10
var REQUEST_PERMIN = 3

//  TODO var TOKEN_BUCKET config

func ReadConfig(filename string) error {

	jsonFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {

		}
	}(jsonFile)

	byteValue, _ := io.ReadAll(jsonFile)

	var result map[string]int
	err = json.Unmarshal([]byte(byteValue), &result)
	loadValues(result)
	if err != nil {
		return err
	}
	return nil
}

func loadValues(data map[string]int) {
	MEMTABLE_CAPACITY = data["memtable_capacity"]
	MEMTABLE_THRESHOLD = data["memtable_threshold"]
	MEMTABLE_STRUCTURE = data["memtable_structure"] /// 0 - skip list   1 - b-tree
	SSTABLE_MULTIPLE_FILES = data["sstable_multiple_files"]
	SSTABLE_SEGMENT_SIZE = data["sstable_segment_size"]
	LSM_DEPTH = data["lsm_depth"]
	CACHE_SIZE = data["cache_size"]
	WAL_SEGMENT_SIZE = data["wal_segment_size"]
	REQUEST_PERMIN = data["request_permin"]
}
