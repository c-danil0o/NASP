package main

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/edsrzf/mmap-go"
)

// Test cases
func (wal *SegmentedWAL) inputTests() error {
	record1 := LogRecord{
		Tombstone: 0,
		Key:       []byte("aleksa"),
		Value:     []byte("perovic"),
	}

	record2 := LogRecord{
		Tombstone: 0,
		Key:       []byte("marko"),
		Value:     []byte("stojanovic"),
	}

	record3 := LogRecord{
		Tombstone: 1,
		Key:       []byte("danilo"),
		Value:     []byte("cvijetic"),
	}

	if err := wal.writeWal(record1); err != nil {
		return err
	}
	wal.currentSize++

	if err := wal.writeWal(record2); err != nil {
		fmt.Println(err)
		return err
	}
	wal.currentSize++

	if err := wal.writeWal(record3); err != nil {
		fmt.Println(err)
		return err
	}
	wal.currentSize++

	return nil
}

// Prints all logs
func (wal *SegmentedWAL) printLogs() {
	current := 0
	for current < wal.segmentCount {
		// fmt.Println(WAL_STR + strconv.Itoa(current) + LOG_STR)
		file, err := os.OpenFile(WAL_STR+strconv.Itoa(current)+LOG_STR, os.O_RDONLY, 0400)
		if err != nil {
			fmt.Println(err)
			break
		}
		// Memory map the file
		data, err := mmap.Map(file, mmap.RDONLY, 0)
		if err != nil {
			fmt.Println(err)
			break
		}
		defer data.Unmap()

		fmt.Print("\n---log broj ", current+1, "---\n")
		err = readRecords(data)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			fmt.Println("Dosli smo do kraja trenutnog fajla.")
		} else if err == fmt.Errorf("verification") {
			fmt.Println("CRC verifikacija je neuspjesna.\nOvaj fajl je korumpiran.")
			current++
		} else if err != nil {
			fmt.Println(err)
			break
		}
		current++
		fmt.Print("\n---kraj log-a---\n\n")
		if current >= wal.segmentCount {
			fmt.Print("\n***Uspjesno su ispisani svi podaci iz svih logova***\n\n")
		}
	}
}

func main() {
	// Creating segmented WAL
	wal := &SegmentedWAL{
		currentSegment: nil,
		segmentCount:   0,
		currentSize:    0,
	}

	// Open the WAL file (for seeing why permission is set to 0600 visit https://phoenixnap.com/kb/linux-file-permissions)
	var err error
	wal.currentSegment, err = os.OpenFile(WAL_STR+"0"+LOG_STR, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer wal.currentSegment.Close()
	wal.segmentCount++

	// Putting all test cases into log files
	wal.inputTests()

	// Going through all logs
	wal.printLogs()
}
