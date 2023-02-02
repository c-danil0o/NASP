package main

import (
	"fmt"
	"os"

	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
	"github.com/c-danil0o/NASP/Finder"
	mt "github.com/c-danil0o/NASP/Memtable"
	wal "github.com/c-danil0o/NASP/WAL"
)

func errorMsg() {
	fmt.Println("Doslo je do greske, molimo pokusajte ponovo.")
}

func menu() int {
	for {
		fmt.Println("Select an option:")
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. List")
		fmt.Println("5. Range Scan")
		fmt.Println("0. Izlaz")
		fmt.Print(">> ")

		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 0:
			fmt.Println("Izlaz iz aplikacije...")
			os.Exit(0)
		case 1:
			if !put() {
				errorMsg()
			} else {
				fmt.Println("Uspjesno ste dodali zapis.")
			}
		case 2:
			if record := get(); record != nil {
				if record.Tombstone != 2 {
					fmt.Printf("Key: %s\n", record.Key)
					fmt.Printf("Value: %s\n", record.Value)
					fmt.Printf("Timestamp: %d\n", record.Timestamp)
					fmt.Printf("Tombstone: %d\n", record.Tombstone)
				} else {
					fmt.Println("Trazeni rekord nije pronadjen.")
				}
			} else {
				errorMsg()
			}
		case 3:
			if delete() {
				fmt.Println("Uneseni rekord je uspjesno izbrisan.")
			}
		case 4:
			// TODO:
			fmt.Println("Listanje")
		case 5:
			// TODO:
			fmt.Println("Range scan")
		default:
			fmt.Println("Neispravan unos. Pokusajte ponovo.")
		}
		fmt.Println()
	}
}

func put() bool {
	var key string
	fmt.Print("\nUnesite kljuc: ")
	n, err := fmt.Scanf("%s", &key)
	if err != nil || n != 1 {
		return false
	}

	var val []byte
	fmt.Print("Unesite vrijednost: ")
	_, err = fmt.Scanf("%s", &val)
	if err != nil {
		return false
	}

	if err := wal.Active.WriteRecord(wal.LogRecord{Tombstone: 0, Key: []byte(key), Value: val}); err != nil {
		return false
	}

	if err := mt.Active.Add(mt.Element{Key: []byte(key), Value: val, Tombstone: 0}); err != nil {
		return false
	}

	return true
}

func get() *container.Element {
	var key string
	fmt.Print("\nUnesite kljuc: ")
	n, err := fmt.Scanf("%s", &key)
	if err != nil || n != 1 {
		return nil
	}

	// Searching in memtable
	memRes := mt.Active.Find(key)
	if memRes != nil && memRes.Tombstone != 2 { // then we have valid return value
		return memRes
	}

	// If not found in memtable
	found, retVal, err := Finder.FindKey([]byte(key))
	if err != nil {
		fmt.Println(err)
		return nil
	} else if !found {
		return &container.Element{Tombstone: 2}
	} else {
		// TODO: ucitati u cache
		return retVal
	}
}

func delete() bool {
	if record := Get(); record != nil {
		if record.Tombstone != 2 {
			if err := wal.Active.WriteRecord(wal.LogRecord{Tombstone: 1, Key: record.Key, Value: record.Value}); err != nil {
				errorMsg()
				return false
			}
			mt.Active.Delete(record.Key)
			return true
		}
		fmt.Println("Trazeni rekord ne postoji.")
		return false
	}
	errorMsg()
	return false
}

func main() {
	config.ReadConfig("config.json")

	wal.Init()
	mt.Init()
	menu()
}
