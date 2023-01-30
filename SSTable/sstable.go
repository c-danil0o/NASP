package SSTable

import (
	"encoding/binary"
	bloomfilter "github.com/c-danil0o/NASP/BloomFilter"
	skiplist "github.com/c-danil0o/NASP/SkipList"
	"math"
	"os"
	"strconv"
)

const SegmentSize = 3

type SSTable struct {
	dataFilename     string
	indexFilename    string // contains all indexes
	summaryFilename  string // contains sample of indexes
	filterFilename   string
	TOCFilename      string
	metadataFilename string
	generation       uint32
	SegmentSize      uint
	NumOfSegments    int
	RecordSize       uint
	DataSize         uint64
	Bloom            bloomfilter.BloomFilter
}

func NewSSTable(dataSize uint64, generation uint32) *SSTable {
	return &SSTable{
		dataFilename:     "usertable-" + strconv.Itoa(int(generation)) + "-Data.db",
		indexFilename:    "usertable-" + strconv.Itoa(int(generation)) + "-Index.db",
		summaryFilename:  "usertable-" + strconv.Itoa(int(generation)) + "-Summary.db",
		filterFilename:   "usertable-" + strconv.Itoa(int(generation)) + "-Filter.db",
		TOCFilename:      "usertable-" + strconv.Itoa(int(generation)) + "-TOC.db",
		metadataFilename: "usertable-" + strconv.Itoa(int(generation)) + "-Metadata.db",
		generation:       generation,
		DataSize:         dataSize,
		SegmentSize:      SegmentSize,
	}
}

func Init(nodes []*skiplist.SkipNode, generation uint32) error {
	count := 0
	count2 := 0
	sstable := NewSSTable(uint64(len(nodes)), generation)
	index := NewIndex(sstable)
	dataFile, _ := os.OpenFile(sstable.dataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	indexFile, _ := os.OpenFile(sstable.indexFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	summaryFile, _ := os.OpenFile(sstable.summaryFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	bloomFile, _ := os.OpenFile(sstable.filterFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	//metadataFile, _ := os.OpenFile(sstable.metadataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)

	defer dataFile.Close()
	defer indexFile.Close()
	defer summaryFile.Close()
	var offset uint64 = 0 // configure offset
	var indexOffset uint64 = 0
	var summarySize = int(math.Ceil(float64(len(nodes)) / float64(sstable.SegmentSize)))
	sstable.NumOfSegments = summarySize
	summary := NewSummary(int32(summarySize))
	sstable.Bloom = *bloomfilter.NewBloomFilter(len(nodes)+50, 0.1)
	for count < len(nodes) {
		for i := 0; i < int(sstable.SegmentSize) && count < len(nodes); i++ {
			sstable.Bloom.Add(nodes[count].Key)
			if i == 0 {
				summary.keys[count2] = nodes[count].Key
				summary.positions[count2] = indexOffset

				count2++
			}
			index.keys[count] = nodes[count].Key
			index.positions[count] = offset
			indexOffset += 8 + uint64(len(nodes[count].Key)) + 8 // keylength + uint64 - position
			var timestampConvert [16]byte
			binary.PutVarint(timestampConvert[:], nodes[count].Timestamp)
			tempRecord := Record{
				CRC:       0,
				Timestamp: timestampConvert,
				Tombstone: nodes[count].Tombstone,
				KeySize:   0,
				ValueSize: 0,
				Key:       nodes[count].Key,
				Value:     nodes[count].Value,
			}

			if err := tempRecord.Write(dataFile); err != nil {
				return err
			}
			offset += tempRecord.RecordSize()
			count += 1

		}
		if err := dataFile.Sync(); err != nil {
			return err
		}

	}
	err := index.WriteIndex(indexFile)
	if err != nil {
		return err
	}
	err = summary.WriteSummary(summaryFile, index.keys[len(index.keys)-1])
	if err != nil {
		return err
	}
	err = sstable.Bloom.Serialize(bloomFile)
	if err != nil {
		return err
	}
	indexFile.Sync()
	summaryFile.Sync()
	bloomFile.Sync()
	return nil
}

func ReadData(file *os.File, offset int64) (*Record, error) {
	_, err := file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	record := Record{}
	err = record.Read(file)
	if err != nil {
		return nil, err
	}
	return &record, nil
}
