package SSTable

import (
	"encoding/binary"
	bloomfilter "github.com/c-danil0o/NASP/BloomFilter"
	skiplist "github.com/c-danil0o/NASP/SkipList"
	"math"
	"os"
)

type SSTable struct {
	dataFilename     string
	indexFilename    string // contains all indexes
	summaryFilename  string // contains sample of indexes
	filterFilename   string
	TOCFilename      string
	metadataFilename string
	generation       uint
	SegmentSize      uint
	NumOfSegments    int
	RecordSize       uint
	DataSize         uint64
	Bloom            bloomfilter.BloomFilter
}

func NewSSTable(dataSize uint64) *SSTable {
	return &SSTable{
		dataFilename:     "usertable-" + "0" + "-Data.db",
		indexFilename:    "usertable-" + "0" + "-Index.db",
		summaryFilename:  "usertable-" + "0" + "-Summary.db",
		filterFilename:   "usertable-" + "0" + "-Filter.db",
		TOCFilename:      "usertable-" + "0" + "-TOC.db",
		metadataFilename: "usertable-" + "0" + "-Metadata.db",
		generation:       0,
		DataSize:         dataSize,
		SegmentSize:      3,
	}
}

func Init(nodes []*skiplist.SkipNode) error {
	count := 0
	count2 := 0
	sstable := NewSSTable(uint64(len(nodes)))
	index := NewIndex(sstable)
	dataFile, _ := os.OpenFile(sstable.dataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	indexFile, _ := os.OpenFile(sstable.indexFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	summaryFile, _ := os.OpenFile(sstable.summaryFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	bloomFile, _ := os.OpenFile(sstable.filterFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	defer dataFile.Close()
	defer indexFile.Close()
	defer summaryFile.Close()
	var offset uint64 = 0 // configure offset
	var indexOffset uint64 = 0
	var summarySize = int(math.Ceil(float64(len(nodes)) / float64(sstable.SegmentSize)))
	sstable.NumOfSegments = summarySize
	summary := NewSummary(summarySize)
	sstable.Bloom = *bloomfilter.NewBloomFilter(len(nodes)+50, 0.1)
	for count < len(nodes) {
		for i := 0; i < int(sstable.SegmentSize) && count < len(nodes); i++ {
			sstable.Bloom.Add(nodes[count].Key)
			if i == 0 {
				summary.keys[count2] = nodes[count].Key
				summary.positions[count2] = indexOffset
				indexOffset += uint64(len(nodes[i].Key)) + 8 // keylength + uint64 - position
				count2++
			}
			index.keys[count] = nodes[count].Key
			index.positions[count] = offset
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
			offset += tempRecord.RecordSize()
			if err := tempRecord.Write(dataFile); err != nil {
				return err
			}
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
	err = summary.WriteSummary(summaryFile)
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

func (sst *SSTable) generateData() {

}
