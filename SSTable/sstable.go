package SSTable

import (
	"bytes"
	"encoding/binary"
	bloomfilter "github.com/c-danil0o/NASP/BloomFilter"
	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
	"io"
	"math"
	"os"
	"strconv"
)

type SSTable struct {
	dataFilename     string
	indexFilename    string // contains all indexes
	summaryFilename  string // contains sample of indexes
	filterFilename   string
	TOCFilename      string
	SSTableFilename  string
	metadataFilename string
	generation       uint32
	SegmentSize      uint
	NumOfSegments    int
	RecordSize       uint
	DataSize         uint64
	Bloom            bloomfilter.BloomFilter
	SSTableMultiple  int
}

func NewSSTable(dataSize uint64, generation uint32) *SSTable {
	return &SSTable{
		dataFilename:     "usertable-" + strconv.Itoa(int(generation)) + "-Data.db",
		indexFilename:    "usertable-" + strconv.Itoa(int(generation)) + "-Index.db",
		summaryFilename:  "usertable-" + strconv.Itoa(int(generation)) + "-Summary.db",
		filterFilename:   "usertable-" + strconv.Itoa(int(generation)) + "-Filter.db",
		TOCFilename:      "usertable-" + strconv.Itoa(int(generation)) + "-TOC.txt",
		metadataFilename: "usertable-" + strconv.Itoa(int(generation)) + "-Metadata.db",
		generation:       generation,
		DataSize:         dataSize,
		SegmentSize:      uint(config.SSTABLE_SEGMENT_SIZE),
		SSTableFilename:  "usertable-" + strconv.Itoa(int(generation)) + "-.db",
		SSTableMultiple:  config.SSTABLE_MULTIPLE_FILES,
	}
}

func Init(nodes []container.DataNode, generation uint32) error {
	if config.SSTABLE_MULTIPLE_FILES == 1 {
		count := 0
		count2 := 0
		sstable := NewSSTable(uint64(len(nodes)), generation)
		index := NewIndex(sstable)
		dataFile, _ := os.OpenFile(sstable.dataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		indexFile, _ := os.OpenFile(sstable.indexFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		summaryFile, _ := os.OpenFile(sstable.summaryFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		bloomFile, _ := os.OpenFile(sstable.filterFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		metadataFile, _ := os.OpenFile(sstable.metadataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		tocFile, _ := os.OpenFile(sstable.TOCFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		sstable.makeTOC(tocFile)
		merkleBuffer := make([]Record, len(nodes))
		defer dataFile.Close()
		defer indexFile.Close()
		defer summaryFile.Close()
		defer metadataFile.Close()
		var offset uint64 = 0 // configure offset
		var indexOffset uint64 = 0
		var summarySize = int(math.Ceil(float64(len(nodes)) / float64(sstable.SegmentSize)))
		sstable.NumOfSegments = summarySize
		summary := NewSummary(int32(summarySize))
		sstable.Bloom = *bloomfilter.NewBloomFilter(len(nodes)+50, 0.1)
		for count < len(nodes) {
			for i := 0; i < int(sstable.SegmentSize) && count < len(nodes); i++ {
				sstable.Bloom.Add(nodes[count].Key())
				if i == 0 {
					summary.keys[count2] = nodes[count].Key()
					summary.positions[count2] = indexOffset

					count2++
				}
				index.keys[count] = nodes[count].Key()
				index.positions[count] = offset
				indexOffset += 8 + uint64(len(nodes[count].Key())) + 8 // keylength + uint64 - position
				var timestampConvert [16]byte
				binary.PutVarint(timestampConvert[:], nodes[count].Timestamp())
				tempRecord := Record{
					CRC:       0,
					Timestamp: timestampConvert,
					Tombstone: nodes[count].Tombstone(),
					KeySize:   0,
					ValueSize: 0,
					Key:       nodes[count].Key(),
					Value:     nodes[count].Value(),
				}
				merkleBuffer[count] = tempRecord
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
		_, err = summary.WriteSummary(summaryFile, index.keys[len(index.keys)-1])
		if err != nil {
			return err
		}
		_, err = sstable.Bloom.Serialize(bloomFile)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		merkleRoot := GenerateMerkle(merkleBuffer)
		err = SerializeMerkleNodes(metadataFile, &buf, merkleRoot.Root)
		if err != nil {
			return err
		}
		metadataFile.Sync()
		indexFile.Sync()
		summaryFile.Sync()
		bloomFile.Sync()
		return nil
	} else {
		err := InitSingle(nodes, generation)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func InitSingle(nodes []container.DataNode, generation uint32) error {
	count := 0
	count2 := 0
	sstable := NewSSTable(uint64(len(nodes)), generation)
	index := NewIndex(sstable)
	dataFile, _ := os.OpenFile(sstable.SSTableFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	merkleBuffer := make([]Record, len(nodes))
	defer dataFile.Close()
	head := make(map[string]int64)
	dataFile.Write(make([]byte, 40))
	var offset uint64 = uint64(40) // configure offset
	head["data"] = int64(offset)   /// beginning of data segment
	var indexOffset uint64 = 0
	var summarySize = int(math.Ceil(float64(len(nodes)) / float64(sstable.SegmentSize)))
	sstable.NumOfSegments = summarySize
	summary := NewSummary(int32(summarySize))
	sstable.Bloom = *bloomfilter.NewBloomFilter(len(nodes)+50, 0.1)
	for count < len(nodes) {
		for i := 0; i < int(sstable.SegmentSize) && count < len(nodes); i++ {
			sstable.Bloom.Add(nodes[count].Key())
			if i == 0 {
				summary.keys[count2] = nodes[count].Key()
				summary.positions[count2] = indexOffset

				count2++
			}
			index.keys[count] = nodes[count].Key()
			index.positions[count] = offset
			indexOffset += 8 + uint64(len(nodes[count].Key())) + 8 // keylength + uint64 - position
			var timestampConvert [16]byte
			binary.PutVarint(timestampConvert[:], nodes[count].Timestamp())
			tempRecord := Record{
				CRC:       0,
				Timestamp: timestampConvert,
				Tombstone: nodes[count].Tombstone(),
				KeySize:   0,
				ValueSize: 0,
				Key:       nodes[count].Key(),
				Value:     nodes[count].Value(),
			}
			merkleBuffer[count] = tempRecord
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
	_, err2 := dataFile.Seek(int64(offset), 0)
	if err2 != nil {
		return err2
	}
	err := index.WriteIndex(dataFile)
	if err != nil {
		return err
	}
	head["index"] = int64(offset)                 // beginning of index segment
	head["summary"] = int64(offset + indexOffset) // beginning of summary segment

	summary.UpdateOffset(offset)
	dataFile.Seek(head["summary"], 0)
	summaryOffset, err := summary.WriteSummary(dataFile, index.keys[len(index.keys)-1])
	if err != nil {
		return err
	}
	head["filter"] = head["summary"] + summaryOffset // beginning of filter segment
	dataFile.Seek(head["filter"], 0)
	bloomsize, err := sstable.Bloom.Serialize(dataFile)
	head["metadata"] = head["filter"] + bloomsize
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	merkleRoot := GenerateMerkle(merkleBuffer)
	dataFile.Seek(head["metadata"], 0)
	err = SerializeMerkleNodes(dataFile, &buf, merkleRoot.Root)
	if err != nil {
		return err
	}

	dataFile.Seek(0, 0)
	buf.Reset()

	err = binary.Write(&buf, binary.BigEndian, head["data"])
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.BigEndian, head["index"])
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.BigEndian, head["summary"])
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.BigEndian, head["filter"])
	if err != nil {
		return err
	}

	err = binary.Write(&buf, binary.BigEndian, head["metadata"])
	if err != nil {
		return err
	}
	dataFile.Write(buf.Bytes())
	dataFile.Sync()
	return nil
}

func ReadHead(file *os.File) (map[string]int64, error) {
	_, err := file.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	result := make(map[string]int64)
	var data int64
	if err := binary.Read(file, binary.BigEndian, &data); err != nil {
		return nil, err
	}
	result["data"] = data
	if err := binary.Read(file, binary.BigEndian, &data); err != nil {
		return nil, err
	}
	result["index"] = data
	if err := binary.Read(file, binary.BigEndian, &data); err != nil {
		return nil, err
	}
	result["summary"] = data
	if err := binary.Read(file, binary.BigEndian, &data); err != nil {
		return nil, err
	}
	result["filter"] = data
	if err := binary.Read(file, binary.BigEndian, &data); err != nil {
		return nil, err
	}
	result["metadata"] = data

	return result, nil

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

func (sst *SSTable) makeTOC(writer io.Writer) error {
	writer.Write([]byte("data:" + sst.dataFilename + "\n"))
	writer.Write([]byte("index:" + sst.indexFilename + "\n"))
	writer.Write([]byte("filter:" + sst.filterFilename + "\n"))
	writer.Write([]byte("summary:" + sst.summaryFilename + "\n"))
	writer.Write([]byte("metadata:" + sst.metadataFilename + "\n"))
	return nil
}
