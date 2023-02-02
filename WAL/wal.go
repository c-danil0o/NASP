package WAL

import (
	"fmt"
	"io"
	"os"
	"strconv"

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
			break
		}

		// Memory map the file
		data, err := mmap.Map(file, mmap.RDONLY, 0)
		if err != nil {
			fmt.Println(err)
			file.Close()
			break
		}

		fmt.Print("\n---log broj ", current+1, "---\n")
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

func Init() { // *SegmentedWAL {
	// Removing all previous logs
	counter := 0
	for {
		if e := os.Remove(WAL_STR + strconv.Itoa(counter) + LOG_STR); e != nil {
			break
		}
		counter++
	}

	Active = *CreateSegmentedLog(config.WAL_SEGMENT_SIZE)
}

func CreateSegmentedLog(max_segment_size int) *SegmentedWAL {
	return &SegmentedWAL{
		CurrentSegment: nil,
		SegmentCount:   0,
		CurrentSize:    0,
		SegmentSize:    max_segment_size,
	}
}
