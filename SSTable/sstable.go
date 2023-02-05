package SSTable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	bloomfilter "github.com/c-danil0o/NASP/BloomFilter"
	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
)

type SSTable struct {
	dataFilename     string
	indexFilename    string // contains all indexes
	summaryFilename  string // contains sample of indexes
	filterFilename   string
	TOCFilename      string
	SSTableFilename  string
	metadataFilename string
	Generation       uint32
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
		Generation:       generation,
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
		tocFile.Close()
		merkleBuffer := make([]Record, len(nodes))
		var bf bytes.Buffer
		binary.Write(&bf, binary.BigEndian, int64(len(nodes)))
		dataFile.Write(bf.Bytes())
		//defer dataFile.Close()
		//defer indexFile.Close()
		//defer summaryFile.Close()
		//defer metadataFile.Close()
		var offset uint64 = 8 // configure offset  + head
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
				//var timestampConvert [16]byte
				//binary.PutVarint(timestampConvert[:], nodes[count].Timestamp())
				tempRecord := Record{
					CRC:       0,
					timestamp: nodes[count].Timestamp(),
					tombstone: nodes[count].Tombstone(),
					KeySize:   0,
					ValueSize: 0,
					key:       nodes[count].Key(),
					value:     nodes[count].Value(),
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
		merkleRoot := GenerateMerkle(merkleBuffer)
		err = merkleRoot.SerializeMerkle(metadataFile)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		dataFile.Close()
		indexFile.Close()
		summaryFile.Close()
		metadataFile.Close()
		return nil
	} else {
		err := InitSingle(nodes, generation)
		if err != nil {
			return err
		}
		return nil
	}
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
	dataFile.Write(make([]byte, 48))
	var offset uint64 = uint64(48) // configure offset
	head["size"] = int64(len(nodes))
	head["data"] = int64(offset) /// beginning of data segment
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
				timestamp: nodes[count].Timestamp(),
				tombstone: nodes[count].Tombstone(),
				KeySize:   0,
				ValueSize: 0,
				key:       nodes[count].Key(),
				value:     nodes[count].Value(),
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

	merkleRoot := GenerateMerkle(merkleBuffer)
	dataFile.Seek(head["metadata"], 0)
	err = merkleRoot.SerializeMerkle(dataFile)
	if err != nil {
		return err
	}

	dataFile.Seek(0, 0)
	var buf bytes.Buffer
	err = binary.Write(&buf, binary.BigEndian, head["size"])
	if err != nil {
		return err
	}

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
	dataFile.Close()
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
	result["size"] = data
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
func ReadSize(file *os.File) (int64, error) {
	_, err := file.Seek(0, 0)
	if err != nil {
		return 0, err
	}
	var data int64
	if err := binary.Read(file, binary.BigEndian, &data); err != nil {
		return 0, err
	}
	return data, nil
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
func ReadTOC(filename string) (map[string]string, error) {
	result := make(map[string]string)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	//defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		temp := strings.Split(scanner.Text(), ":")
		result[temp[0]] = temp[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	file.Close()
	return result, nil
}

func RemoveFiles(toc map[string]string) error {
	err := os.Remove(toc["data"])
	if err != nil {
		return err
	}
	err = os.Remove(toc["index"])
	if err != nil {
		return err
	}
	err = os.Remove(toc["filter"])
	if err != nil {
		return err
	}
	err = os.Remove(toc["metadata"])
	if err != nil {
		return err
	}
	err = os.Remove(toc["summary"])
	if err != nil {
		return err
	}
	return nil

}
func Merge(sst1gen int, sst2gen int, generation int) (error, int) {
	if config.SSTABLE_MULTIPLE_FILES == 1 { // sst1 i sst2 su TOC
		sst1toc := "usertable-" + strconv.Itoa(sst1gen) + "-TOC.txt"
		sst2toc := "usertable-" + strconv.Itoa(sst2gen) + "-TOC.txt"
		file1, err := ReadTOC(sst1toc)
		if err != nil {
			return err, 0
		}
		file2, err := ReadTOC(sst2toc)
		if err != nil {
			return err, 0
		}

		dataFile1, _ := os.OpenFile(file1["data"], os.O_RDONLY, 0600)
		//defer dataFile1.Close()

		dataFile2, _ := os.OpenFile(file2["data"], os.O_RDONLY, 0600)
		//defer dataFile2.Close()

		fmt.Println("\n\n", dataFile1.Name(), "-", dataFile2.Name(), "\n\n")

		size1, err := ReadSize(dataFile1)
		size2, err := ReadSize(dataFile2)
		if err != nil {
			return err, 0
		}
		dataSize := size1 + size2
		sstable := NewSSTable(uint64(dataSize), uint32(generation))

		index := NewIndex(sstable)
		dataFile, _ := os.OpenFile(sstable.dataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		indexFile, _ := os.OpenFile(sstable.indexFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		summaryFile, _ := os.OpenFile(sstable.summaryFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		bloomFile, _ := os.OpenFile(sstable.filterFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		metadataFile, _ := os.OpenFile(sstable.metadataFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		tocFile, _ := os.OpenFile(sstable.TOCFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		sstable.makeTOC(tocFile)
		size := make([]byte, 8)
		dataFile.Write(size)
		merkleBuffer := make([]Record, dataSize)
		//defer dataFile.Close()
		//defer indexFile.Close()
		//defer summaryFile.Close()
		//defer metadataFile.Close()
		var offset uint64 = 8 // configure offset
		var indexOffset uint64 = 0
		var summarySize = int(math.Ceil(float64(dataSize) / float64(sstable.SegmentSize)))
		sstable.NumOfSegments = summarySize
		summary := NewSummary(int32(summarySize))
		sstable.Bloom = *bloomfilter.NewBloomFilter(int(dataSize+10), 0.1)
		var count = 0
		var count2 = 0
		var record1 Record
		var record2 Record
		var flush = 0
		if !record1.ReadNext(dataFile1) {
			flush = 2
		}
		if !record2.ReadNext(dataFile2) {
			flush = 1
		}
		var writeRecord Record

		for true {
			if flush != 0 {
				if flush == 1 {
					if err := record1.Write(dataFile); err != nil { // zapisan u novi fajl
						return err, 0
					}
					sstable.Bloom.Add(record1.Key())
					if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
						summary.keys[count2] = record1.Key()
						summary.positions[count2] = indexOffset
						count2++
					}
					index.keys[count] = record1.Key()
					index.positions[count] = offset
					offset += record1.RecordSize()
					indexOffset += 8 + uint64(len(record1.Key())) + 8 // keylength + uint64 - position
					count++

					for record1.ReadNext(dataFile1) {
						if err := record1.Write(dataFile); err != nil { // zapisan u novi fajl
							return err, 0
						}
						sstable.Bloom.Add(record1.Key())
						if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
							summary.keys[count2] = record1.Key()
							summary.positions[count2] = indexOffset
							count2++
						}
						index.keys[count] = record1.Key()
						index.positions[count] = offset
						offset += record1.RecordSize()
						indexOffset += 8 + uint64(len(record1.Key())) + 8 // keylength + uint64 - position
						count++

					}
				} else {
					if err := record2.Write(dataFile); err != nil { // zapisan u novi fajl
						return err, 0
					}
					sstable.Bloom.Add(record2.Key())
					if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
						summary.keys[count2] = record2.Key()
						summary.positions[count2] = indexOffset
						count2++
					}
					index.keys[count] = record2.Key()
					index.positions[count] = offset
					offset += record2.RecordSize()
					indexOffset += 8 + uint64(len(record2.Key())) + 8 // keylength + uint64 - position
					count++
					for record2.ReadNext(dataFile2) {
						if err := record2.Write(dataFile); err != nil { // zapisan u novi fajl
							return err, 0
						}
						sstable.Bloom.Add(record2.Key())
						if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
							summary.keys[count2] = record2.Key()
							summary.positions[count2] = indexOffset
							count2++
						}
						index.keys[count] = record2.Key()
						index.positions[count] = offset
						offset += record2.RecordSize()
						indexOffset += 8 + uint64(len(record2.Key())) + 8 // keylength + uint64 - position
						count++
					}
				}
				break
			}
			cmp := bytes.Compare(record1.key, record2.key)
			if cmp == 0 {
				if record1.timestamp > record2.timestamp {
					writeRecord = record1
					if !record1.ReadNext(dataFile1) {
						flush = 2
					}
					if !record2.ReadNext(dataFile2) {
						flush = 1
					}

				} else {
					writeRecord = record2
					if !record2.ReadNext(dataFile2) {
						flush = 1
					}
					if !record1.ReadNext(dataFile1) {
						flush = 2
					}
				}
			} else if cmp == -1 {
				writeRecord = record1
				if !record1.ReadNext(dataFile1) {
					flush = 2
				}

			} else if cmp == 1 {
				writeRecord = record2
				if !record2.ReadNext(dataFile2) {
					flush = 1
				}
			}
			if err := writeRecord.Write(dataFile); err != nil { // zapisan u novi fajl
				return err, 0
			}
			sstable.Bloom.Add(writeRecord.Key())
			if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
				summary.keys[count2] = writeRecord.Key()
				summary.positions[count2] = indexOffset
				count2++
			}
			index.keys[count] = writeRecord.Key()
			index.positions[count] = offset
			offset += writeRecord.RecordSize()
			indexOffset += 8 + uint64(len(writeRecord.Key())) + 8 // keylength + uint64 - position
			count++

		}
		dataFile.Seek(0, 0)
		var bf bytes.Buffer
		binary.Write(&bf, binary.BigEndian, int64(count))
		dataFile.Write(bf.Bytes())
		err = index.WriteIndex(indexFile)
		if err != nil {
			return err, 0
		}
		_, err = summary.WriteSummary(summaryFile, index.keys[index.indexSize()-1])
		if err != nil {
			return err, 0
		}
		_, err = sstable.Bloom.Serialize(bloomFile)
		if err != nil {
			return err, 0
		}
		merkleRoot := GenerateMerkle(merkleBuffer)
		err = merkleRoot.SerializeMerkle(metadataFile)
		if err != nil {
			return err, 0
		}
		metadataFile.Close()
		indexFile.Close()
		summaryFile.Close()
		bloomFile.Close()
		tocFile.Close()
		dataFile1.Close()
		dataFile2.Close()

		err = RemoveFiles(file1)
		if err != nil {
			return err, 0
		}
		err = RemoveFiles(file2)
		if err != nil {
			return err, 0
		}
		err = os.Remove(sst1toc)
		if err != nil {
			return err, 0
		}
		err = os.Remove(sst2toc)
		if err != nil {
			return err, 0
		}
		return nil, count

	} else { // sst1 i sst2 su usertable
		sst1 := "usertable-" + strconv.Itoa(sst1gen) + "-.db"
		sst2 := "usertable-" + strconv.Itoa(sst2gen) + "-.db"

		dataFile1, _ := os.OpenFile(sst1, os.O_RDONLY, 0600)
		head1, _ := ReadHead(dataFile1)
		dataFile2, _ := os.OpenFile(sst2, os.O_RDONLY, 0600)
		head2, _ := ReadHead(dataFile2)

		dataSize := head1["size"] + head2["size"]
		sstable := NewSSTable(uint64(dataSize), uint32(generation))
		dataFile, _ := os.OpenFile(sstable.SSTableFilename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
		index := NewIndex(sstable)
		head := make(map[string]int64)
		dataFile.Write(make([]byte, 48))
		var offset uint64 = uint64(48) // configure offset
		head["size"] = dataSize
		head["data"] = int64(offset) /// beginning of data segment
		merkleBuffer := make([]Record, dataSize)
		//defer dataFile.Close()
		var indexOffset uint64 = 0
		var summarySize = int(math.Ceil(float64(dataSize) / float64(sstable.SegmentSize)))
		sstable.NumOfSegments = summarySize
		summary := NewSummary(int32(summarySize))
		sstable.Bloom = *bloomfilter.NewBloomFilter(int(dataSize+10), 0.1)
		var count = 0
		var count2 = 0
		var record1 Record
		var record2 Record
		var flush = 0

		dataFile1.Seek(head1["data"], 0)
		dataFile2.Seek(head2["data"], 0)

		if !record1.ReadNextSingle(dataFile1, head1["index"]) {
			flush = 2
		}
		if !record2.ReadNextSingle(dataFile2, head2["index"]) {
			flush = 1
		}
		var writeRecord Record

		for true {
			if flush != 0 {
				if flush == 1 {
					if err := record1.Write(dataFile); err != nil { // zapisan u novi fajl
						return err, 0
					}
					sstable.Bloom.Add(record1.Key())
					if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
						summary.keys[count2] = record1.Key()
						summary.positions[count2] = indexOffset
						count2++
					}
					index.keys[count] = record1.Key()
					index.positions[count] = offset
					offset += record1.RecordSize()
					indexOffset += 8 + uint64(len(record1.Key())) + 8 // keylength + uint64 - position
					count++

					for record1.ReadNextSingle(dataFile1, head1["index"]) {
						if err := record1.Write(dataFile); err != nil { // zapisan u novi fajl
							return err, 0
						}
						sstable.Bloom.Add(record1.Key())
						if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
							summary.keys[count2] = record1.Key()
							summary.positions[count2] = indexOffset
							count2++
						}
						index.keys[count] = record1.Key()
						index.positions[count] = offset
						offset += record1.RecordSize()
						indexOffset += 8 + uint64(len(record1.Key())) + 8 // keylength + uint64 - position
						count++

					}
				} else {
					if err := record2.Write(dataFile); err != nil { // zapisan u novi fajl
						return err, 0
					}
					sstable.Bloom.Add(record2.Key())
					if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
						summary.keys[count2] = record2.Key()
						summary.positions[count2] = indexOffset
						count2++
					}
					index.keys[count] = record2.Key()
					index.positions[count] = offset
					offset += record2.RecordSize()
					indexOffset += 8 + uint64(len(record2.Key())) + 8 // keylength + uint64 - position
					count++
					for record2.ReadNextSingle(dataFile2, head2["index"]) {
						if err := record2.Write(dataFile); err != nil { // zapisan u novi fajl
							return err, 0
						}
						sstable.Bloom.Add(record2.Key())
						if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
							summary.keys[count2] = record2.Key()
							summary.positions[count2] = indexOffset
							count2++
						}
						index.keys[count] = record2.Key()
						index.positions[count] = offset
						offset += record2.RecordSize()
						indexOffset += 8 + uint64(len(record2.Key())) + 8 // keylength + uint64 - position
						count++
					}
				}
				break
			}
			cmp := bytes.Compare(record1.key, record2.key)
			if cmp == 0 {
				if record1.timestamp > record2.timestamp {
					writeRecord = record1
					if !record1.ReadNextSingle(dataFile1, head1["index"]) {
						flush = 2
					}
					if !record2.ReadNextSingle(dataFile2, head2["index"]) {
						flush = 1
					}

				} else {
					writeRecord = record2
					if !record2.ReadNextSingle(dataFile2, head2["index"]) {
						flush = 1
					}
					if !record1.ReadNextSingle(dataFile1, head1["index"]) {
						flush = 2
					}
				}
			} else if cmp == -1 {
				writeRecord = record1
				if !record1.ReadNextSingle(dataFile1, head1["index"]) {
					flush = 2
				}

			} else if cmp == 1 {
				writeRecord = record2
				if !record2.ReadNextSingle(dataFile2, head2["index"]) {
					flush = 1
				}
			}
			if err := writeRecord.Write(dataFile); err != nil { // zapisan u novi fajl
				return err, 0
			}
			sstable.Bloom.Add(writeRecord.Key())
			if count%config.SSTABLE_SEGMENT_SIZE == 0 { // summary
				summary.keys[count2] = writeRecord.Key()
				summary.positions[count2] = indexOffset
				count2++
			}
			index.keys[count] = writeRecord.Key()
			index.positions[count] = offset
			offset += writeRecord.RecordSize()
			indexOffset += 8 + uint64(len(writeRecord.Key())) + 8 // keylength + uint64 - position
			count++

		}
		_, err2 := dataFile.Seek(int64(offset), 0)
		if err2 != nil {
			return err2, 0
		}
		err := index.WriteIndex(dataFile)
		if err != nil {
			return err, 0
		}
		head["index"] = int64(offset)                 // beginning of index segment
		head["summary"] = int64(offset + indexOffset) // beginning of summary segment

		summary.UpdateOffset(offset)
		dataFile.Seek(head["summary"], 0)
		summaryOffset, err := summary.WriteSummary(dataFile, index.keys[len(index.keys)-1])
		if err != nil {
			return err, 0
		}
		head["filter"] = head["summary"] + summaryOffset // beginning of filter segment
		dataFile.Seek(head["filter"], 0)
		bloomsize, err := sstable.Bloom.Serialize(dataFile)
		head["metadata"] = head["filter"] + bloomsize
		head["size"] = int64(count)
		if err != nil {
			return err, 0
		}

		merkleRoot := GenerateMerkle(merkleBuffer)
		dataFile.Seek(head["metadata"], 0)
		err = merkleRoot.SerializeMerkle(dataFile)
		if err != nil {
			return err, 0
		}

		dataFile.Seek(0, 0)
		var buf bytes.Buffer

		err = binary.Write(&buf, binary.BigEndian, head["size"])
		if err != nil {
			return err, 0
		}

		err = binary.Write(&buf, binary.BigEndian, head["data"])
		if err != nil {
			return err, 0
		}

		err = binary.Write(&buf, binary.BigEndian, head["index"])
		if err != nil {
			return err, 0
		}

		err = binary.Write(&buf, binary.BigEndian, head["summary"])
		if err != nil {
			return err, 0
		}

		err = binary.Write(&buf, binary.BigEndian, head["filter"])
		if err != nil {
			return err, 0
		}

		err = binary.Write(&buf, binary.BigEndian, head["metadata"])
		if err != nil {
			return err, 0
		}
		dataFile.Write(buf.Bytes())
		dataFile.Close()
		dataFile1.Close()
		dataFile2.Close()
		err = os.Remove(sst1)
		if err != nil {
			return err, 0
		}
		err = os.Remove(sst2)
		if err != nil {
			return err, 0
		}

		return nil, count

	}
}
