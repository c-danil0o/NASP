package Finder

import (
	"bufio"
	"bytes"
	"os"
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
	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		linesplit := strings.Split(line, ":")
		data[linesplit[0]] = linesplit[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return data, nil

}
func FindKey(key []byte) (bool, container.DataNode, error) {
	if config.SSTABLE_MULTIPLE_FILES == 1 {
		filenames, err := readTOC("usertable-0-TOC.txt")
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

		dataFile, _ := os.OpenFile("usertable-0-.db", os.O_RDONLY, 0600)
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

func PrefixScan(key []byte) (bool, []container.DataNode, error) {
	var retVal []container.DataNode
	if config.SSTABLE_MULTIPLE_FILES == 1 {
		filenames, err := readTOC("usertable-0-TOC.txt")
		if err != nil {
			return false, nil, err
		}
		dataFile, _ := os.OpenFile(filenames["data"], os.O_RDONLY, 0600)
		indexFile, _ := os.OpenFile(filenames["index"], os.O_RDONLY, 0600)
		summaryFile, _ := os.OpenFile(filenames["summary"], os.O_RDONLY, 0600)

		first, last, err := SSTable.ReadFirstLast(summaryFile, 0)
		if err != nil {
			return false, nil, err
		}
		if (bytes.Compare(key, first) == 0 || bytes.Compare(key, first) == 1) && (bytes.Compare(key, last) == 0 || bytes.Compare(key, last) == -1) {
			summary, err := SSTable.ReadSummary(summaryFile, 0)
			if err != nil {
				return false, nil, err
			}
			offsets, err := summary.FindPrefixKeys(key)
			if err != nil {
				return false, nil, err
			}
			var dataPosition int64
			var foundRecord *SSTable.Record
			for i := range offsets {
				dataPosition, err = SSTable.FindIDSegment(key, indexFile, offsets[i], config.SSTABLE_SEGMENT_SIZE)
				if err != nil {
					return false, nil, err
				}
				foundRecord, err = SSTable.ReadData(dataFile, dataPosition)
				if err != nil {
					return false, nil, err
				}
				retVal = append(retVal, foundRecord)
			}
			return true, retVal, nil
		}
		return false, nil, nil
	} else {

		dataFile, _ := os.OpenFile("usertable-0-.db", os.O_RDONLY, 0600)
		head, _ := SSTable.ReadHead(dataFile)

		first, last, err := SSTable.ReadFirstLast(dataFile, head["summary"])
		if err != nil {
			return false, nil, err
		}
		if (bytes.Compare(key, first) == 0 || bytes.Compare(key, first) == 1) && (bytes.Compare(key, last) == 0 || bytes.Compare(key, last) == -1) {
			summary, err := SSTable.ReadSummary(dataFile, head["summary"])
			if err != nil {
				return false, nil, err
			}
			offsets, err := summary.FindPrefixKeys(key)
			if err != nil {
				return false, nil, err
			}
			var dataPosition int64
			var foundRecord *SSTable.Record
			for i := range offsets {
				dataPosition, err = SSTable.FindIDSegment(key, dataFile, offsets[i], config.SSTABLE_SEGMENT_SIZE)
				if err != nil {
					return false, nil, err
				}
				foundRecord, err = SSTable.ReadData(dataFile, dataPosition)
				if err != nil {
					return false, nil, err
				}
				retVal = append(retVal, foundRecord)
			}
			return true, retVal, nil
		}
		return false, nil, nil
	}
}
