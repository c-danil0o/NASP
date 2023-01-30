package Finder

import (
	"bufio"
	"bytes"
	bloomfilter "github.com/c-danil0o/NASP/BloomFilter"
	"github.com/c-danil0o/NASP/SSTable"
	"os"
	"strings"
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
func FindKey(key []byte) (bool, *SSTable.Record, error) {
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
			dataPosition, err := SSTable.FindIDSegment(key, indexFile, offset, SSTable.SegmentSize)

			foundRecord, err := SSTable.ReadData(dataFile, dataPosition)

			if err != nil {
				return false, nil, err
			}
			return true, foundRecord, nil
		}
	}
	return false, nil, nil
}
