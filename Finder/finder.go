package Finder

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	container "github.com/c-danil0o/NASP/DataContainer"

	bloomfilter "github.com/c-danil0o/NASP/BloomFilter"
	config "github.com/c-danil0o/NASP/Config"
	"github.com/c-danil0o/NASP/SSTable"
)

func readTOC(filename string) (map[string]string, error) {
	data := make(map[string]string)
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	//defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		linesplit := strings.Split(line, ":")
		data[linesplit[0]] = linesplit[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	f.Close()
	return data, nil

}
func FindKey(key []byte, generation uint32) (bool, container.DataNode, error) { // ovde treba jos jedan parametar - generacija sst
	if config.SSTABLE_MULTIPLE_FILES == 1 { // koja se posmatra
		filenames, err := readTOC("usertable-" + strconv.Itoa(int(generation)) + "-TOC.txt") //izmenimo da se menja filename
		if err != nil {
			return false, nil, err
		}
		dataFile, _ := os.OpenFile(filenames["data"], os.O_RDONLY, 0600)
		indexFile, _ := os.OpenFile(filenames["index"], os.O_RDONLY, 0600)
		summaryFile, _ := os.OpenFile(filenames["summary"], os.O_RDONLY, 0600)
		bloomFile, _ := os.OpenFile(filenames["filter"], os.O_RDONLY, 0600)

		bloom, _ := bloomfilter.Read(bloomFile, 0)
		if !bloom.Find(key) {
			return false, nil, nil
		} else {
			first, last, err := SSTable.ReadFirstLast(summaryFile, 0)
			if err != nil {
				return false, nil, err
			}
			if (bytes.Compare(key, first) == 0 || bytes.Compare(key, first) == 1) && (bytes.Compare(key, last) == 0 || bytes.Compare(key, last) == -1) {
				summary, err := SSTable.ReadSummary(summaryFile, 0)
				if err != nil {
					return false, nil, err
				}
				offset, err := summary.FindKey(key)
				if err != nil {
					return false, nil, err
				}
				dataPosition, err := SSTable.FindIDSegment(key, indexFile, offset, config.SSTABLE_SEGMENT_SIZE)

				foundRecord, err := SSTable.ReadData(dataFile, dataPosition)

				if err != nil {
					return false, nil, err
				}
				return true, foundRecord, nil
			}
		}
		return false, nil, nil
	} else {

		dataFile, _ := os.OpenFile("usertable-"+strconv.Itoa(int(generation))+"-.db", os.O_RDONLY, 0600)
		head, _ := SSTable.ReadHead(dataFile)

		bloom, _ := bloomfilter.Read(dataFile, head["filter"])
		if !bloom.Find(key) {
			return false, nil, nil
		} else {
			first, last, err := SSTable.ReadFirstLast(dataFile, head["summary"])
			if err != nil {
				return false, nil, err
			}
			if (bytes.Compare(key, first) == 0 || bytes.Compare(key, first) == 1) && (bytes.Compare(key, last) == 0 || bytes.Compare(key, last) == -1) {
				summary, err := SSTable.ReadSummary(dataFile, head["summary"])
				if err != nil {
					return false, nil, err
				}
				offset, err := summary.FindKey(key)
				if err != nil {
					return false, nil, err
				}
				dataPosition, err := SSTable.FindIDSegment(key, dataFile, offset, config.SSTABLE_SEGMENT_SIZE)

				foundRecord, err := SSTable.ReadData(dataFile, dataPosition)

				if err != nil {
					return false, nil, err
				}
				return true, foundRecord, nil
			}
		}
		return false, nil, nil
	}
}

func PrefixScan(key []byte, generation uint32) (bool, []container.DataNode, error) { // [] --> map[key] : datanode
	var retVal []container.DataNode
	if config.SSTABLE_MULTIPLE_FILES == 1 {
		filenames, err := readTOC("usertable-" + strconv.Itoa(int(generation)) + "-TOC.txt")
		if err != nil {
			return false, nil, err
		}
		dataFile, _ := os.OpenFile(filenames["data"], os.O_RDONLY, 0600)
		indexFile, _ := os.OpenFile(filenames["index"], os.O_RDONLY, 0600)

		var dataPositions []int64
		var foundRecord *SSTable.Record

		dataPositions = []int64{}
		dataPositions, err = SSTable.FindIDSegmentsMultiple(key, indexFile, 0)
		if err != nil {
			return true, retVal, err
		}
		for j := range dataPositions {
			foundRecord, err = SSTable.ReadData(dataFile, dataPositions[j])
			if err != nil {
				return true, retVal, err
			}
			retVal = append(retVal, foundRecord)
		}

		if len(retVal) == 0 {
			return false, nil, nil
		}
		return true, retVal, nil
	} else {
		dataFile, _ := os.OpenFile("usertable-"+strconv.Itoa(int(generation))+"-.db", os.O_RDONLY, 0600)
		head, _ := SSTable.ReadHead(dataFile)
		var dataPositions []int64
		var foundRecord *SSTable.Record
		offset := head["index"]
		dataPositions, err := SSTable.FindIDSegments(key, dataFile, offset, head["summary"])
		if err != nil {
			fmt.Println("er1", err.Error())
			return true, retVal, err
		}
		for j := range dataPositions {
			foundRecord, err = SSTable.ReadData(dataFile, dataPositions[j])
			if err != nil {
				fmt.Println("er2", err.Error())
				return true, retVal, err
			}
			retVal = append(retVal, foundRecord)
		}
		if len(retVal) == 0 {
			return false, nil, nil
		}
		return true, retVal, nil

	}
}

func RangeScan(minKey []byte, maxKey []byte, generation uint32) (bool, []container.DataNode, error) {
	var retVal []container.DataNode
	if config.SSTABLE_MULTIPLE_FILES == 1 {
		filenames, err := readTOC("usertable-" + strconv.Itoa(int(generation)) + "-TOC.txt")
		if err != nil {
			return false, nil, err
		}
		dataFile, _ := os.OpenFile(filenames["data"], os.O_RDONLY, 0600)
		indexFile, _ := os.OpenFile(filenames["index"], os.O_RDONLY, 0600)

		var dataPositions []int64
		var foundRecord *SSTable.Record

		dataPositions = []int64{}
		dataPositions, err = SSTable.FindRangeIDSegmentsMultiple(minKey, maxKey, indexFile, 0)
		if err != nil {
			return true, retVal, err
		}
		for j := range dataPositions {
			foundRecord, err = SSTable.ReadData(dataFile, dataPositions[j])
			if err != nil {
				return true, retVal, err
			}
			retVal = append(retVal, foundRecord)
		}

		if len(retVal) == 0 {
			return false, nil, nil
		}
		return true, retVal, nil
	} else {
		dataFile, _ := os.OpenFile("usertable-"+strconv.Itoa(int(generation))+"-.db", os.O_RDONLY, 0600)
		head, _ := SSTable.ReadHead(dataFile)
		var dataPositions []int64
		var foundRecord *SSTable.Record
		offset := head["index"]
		dataPositions, err := SSTable.FindRangeIDSegments(minKey, maxKey, dataFile, offset, head["summary"])
		if err != nil {
			return true, retVal, err
		}
		for j := range dataPositions {
			foundRecord, err = SSTable.ReadData(dataFile, dataPositions[j])
			if err != nil {
				return true, retVal, err
			}
			retVal = append(retVal, foundRecord)
		}
		if len(retVal) == 0 {
			return false, nil, nil
		}
		return true, retVal, nil
	}
}
