package WAL

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	config "github.com/c-danil0o/NASP/Config"
	"github.com/edsrzf/mmap-go"
)

// Prints all logs
func (wal *SegmentedWAL) PrintLogs() {
	current := 0
	for current <= wal.SegmentCount {
		// Open first file
		file, err := os.OpenFile(WAL_STR+strconv.Itoa(current)+LOG_STR, os.O_RDONLY, 0400)
		if err != nil {
			fmt.Println(err)
			current++
			continue
		}

		// Memory map the file
		data, err := mmap.Map(file, mmap.RDONLY, 0)
		if err != nil {
			fmt.Println(err)
			file.Close()
			break
		}

		fmt.Print("\n---log broj ", current, "---\n")
		err = AllRecords(data)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			fmt.Println("Dosli smo do kraja trenutnog fajla.")
		} else if err == fmt.Errorf("verification") {
			fmt.Println("CRC verifikacija je neuspjesna.\nOvaj fajl je korumpiran.")
		} else if err != nil {
			fmt.Println(err)
			file.Close()
			break
		}
		fmt.Print("\n---kraj log-a---\n\n")
		current++
		file.Close()
		data.Unmap()
	}
	fmt.Print("\n***Uspjesno su ispisani svi podaci iz svih logova***\n\n")
}

var Active SegmentedWAL

func Init() { // *SegmentedWAL
	aux, err := getCurrentSegment()
	if err != nil {
		fmt.Println(err)
		return
	}
	Active = *CreateSegmentedLog(aux, config.WAL_SEGMENT_SIZE)
}

func CreateSegmentedLog(aux int, max_segment_size int) *SegmentedWAL {
	// Open first file
	file, err := os.OpenFile(WAL_STR+strconv.Itoa(aux)+LOG_STR, os.O_RDONLY, 0400)
	if err != nil {
		fmt.Println(err)
	}

	// Memory map the file
	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		fmt.Println(err)
	}

	// Read elements to see how many there are inside
	var offset uint64
	var counter int
	for offset < uint64(len(data)) {
		record, err := ReadRecord(data[offset:])
		if err != nil {
			fmt.Println(err)
		}
		offset += record.recordSize()
		counter++
	}
	file.Close()
	data.Unmap()

	return &SegmentedWAL{
		CurrentSegment: nil,
		SegmentCount:   aux,
		CurrentSize:    counter,
		SegmentSize:    max_segment_size,
	}
}

func getCurrentSegment() (int, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	max_seg := 0
	// Walking through dir in search of highest value of wal log
	err = filepath.Walk(cwd, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Checking if it's in format wal-num.log and updating highest value
		if strings.HasPrefix(info.Name(), WAL_STR) && strings.HasSuffix(info.Name(), LOG_STR) {
			seg_str := info.Name()[len(WAL_STR) : len(info.Name())-len(LOG_STR)]
			curr_seg, err := strconv.Atoi(seg_str)
			if err != nil {
				return err
			}
			if curr_seg > max_seg {
				max_seg = curr_seg
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return max_seg, nil
}
