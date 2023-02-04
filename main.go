package main

import (
	"bytes"
	"fmt"
	lsmt "github.com/c-danil0o/NASP/LSM"
	"os"
	"strconv"

	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
	lru "github.com/c-danil0o/NASP/LRU"
	mt "github.com/c-danil0o/NASP/Memtable"
	wal "github.com/c-danil0o/NASP/WAL"
)

func errorMsg() {
	fmt.Println("Doslo je do greske, molimo pokusajte ponovo.")
}

func menu() {
	//config.ReadConfig("config.json")
	//
	//wal.Init()
	//mt.Init()
	//lru.Init()
	//lsmt := LSM.NewLSMTree()

	for {
		fmt.Println("Select an option:")
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. List")
		fmt.Println("5. Range Scan")
		fmt.Println("6. Input SSTable tests")
		fmt.Println("7. CMS")
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
			if record, err := get(); err != nil {
				if record != nil {
					fmt.Printf("Key: %s\n", record.Key())
					fmt.Printf("Value: %s\n", record.Value())
					fmt.Printf("Timestamp: %d\n", record.Timestamp())
					fmt.Printf("Tombstone: %d\n", record.Tombstone())
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
			// TODO: paginacija
			resultsPerPage, viewPage := getPaginationInfo()
			if res, err := list(); err != nil {
				if res != nil {
					fmt.Println("\n---Rezultati pretrage---")
					for i := viewPage * resultsPerPage; i < resultsPerPage; i++ {
						if int(i) >= len(res) {
							if i == viewPage*resultsPerPage {
								fmt.Println("Nema rezultata na ovoj stranici.")
							}
							break
						}
						fmt.Println("\n" + strconv.Itoa(int(i+1)) + ".rekord:")
						fmt.Printf("Key: %s\n", res[i].Key())
						fmt.Printf("Value: %s\n", res[i].Value())
						fmt.Printf("Timestamp: %d\n", res[i].Timestamp())
						fmt.Printf("Tombstone: %d\n", res[i].Tombstone())
					}

					fmt.Println("\n---Kraj ispisa---\n")
				} else {
					fmt.Println("Ne postoji rekord cijem kljucu je uneseni string prefiks.")
				}
			} else {
				errorMsg()
			}
		case 5:
			// TODO: paginacija
			resultsPerPage, viewPage := getPaginationInfo()
			if res, err := rangeScan(); err == nil {
				if res != nil {
					fmt.Println("\n---Rezultati pretrage---")
					for i := viewPage * resultsPerPage; i < resultsPerPage; i++ {
						if int(i) >= len(res) {
							if i == viewPage*resultsPerPage {
								fmt.Println("Nema rezultata na ovoj stranici.")
							}
							break
						}
						fmt.Println("\n" + strconv.Itoa(int(i+1)) + ".rekord:")
						fmt.Printf("Key: %s\n", res[i].Key())
						fmt.Printf("Value: %s\n", res[i].Value())
						fmt.Printf("Timestamp: %d\n", res[i].Timestamp())
						fmt.Printf("Tombstone: %d\n", res[i].Tombstone())
					}
					fmt.Println("\n---Kraj ispisa---\n")
				} else {
					fmt.Println("Ne postoji rekord cijem kljucu je uneseni string prefiks.")
				}
			} else {
				if err == fmt.Errorf("minKey > maxKey") {
					fmt.Println("Uneseni minimalni kljuc mora biti manji od unesenog veceg kljuca.")
				} else {
					errorMsg()
				}
			}
		case 6:
			testing()
		case 7:

		default:
			fmt.Println("Neispravan unos. Pokusajte ponovo.")
		}
		fmt.Println()
	}
}

func getPaginationInfo() (uint32, uint32) {
	var resultsPerPage uint32
	var viewPage uint32
	fmt.Println("Unesite koliko zelite rezultata da se prikaze po stranici : ")
	fmt.Scanf("%u", &resultsPerPage)
	fmt.Println("Unesite koju stranicu zelite da pogledate : ")
	fmt.Scanf("%u", &viewPage)
	return resultsPerPage, viewPage
}

func testing() {
	if err := mt.Active.Add([]byte("data1"), []byte("val")); err != nil {
		fmt.Println(err)
	}
	if err := mt.Active.Add([]byte("data5"), []byte("val")); err != nil {
		fmt.Println(err)
	}
	if err := mt.Active.Add([]byte("data3"), []byte("val")); err != nil {
		fmt.Println(err)
	}
	if err := mt.Active.Add([]byte("data18"), []byte("val")); err != nil {
		fmt.Println(err)
	}
	if err := mt.Active.Add([]byte("data19"), []byte("val")); err != nil {
		fmt.Println(err)
	}

	if err := mt.Active.Add([]byte("data6"), []byte("val")); err != nil {
		fmt.Println(err)
	}
	if err := mt.Active.Add([]byte("data7"), []byte("val")); err != nil {
		fmt.Println(err)
	}
	fmt.Println("\nTest cases put successfully")
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

	if err := mt.Active.Add([]byte(key), val); err != nil {
		fmt.Println(err)
		return false
	}

	return true
}

func get() (container.DataNode, error) {
	var key string
	fmt.Print("\nUnesite kljuc: ")
	n, err := fmt.Scanf("%s", &key)
	if err != nil || n != 1 {
		return nil, err
	}

	// Searching in memtable
	retVal := mt.Active.Find(key)
	if retVal != nil { // then we have valid return value
		return retVal, nil
	}

	var found bool
	retVal, found = lru.Active.Find([]byte(key))
	if found {
		return retVal, nil
	}

	// If not found in memtable
	// ovde mi prepravaljamo
	//found, retVal, err = Finder.FindKey([]byte(key))
	found, retVal, err = lsmt.Active.FindKey([]byte(key))
	if err != nil {
		fmt.Println(err)
		return nil, err
	} else if !found {
		return retVal, nil
	} else {
		lru.Active.Insert(retVal)
		return retVal, nil
	}
}

func delete() bool {
	if record, err := get(); err != nil {
		if record != nil {
			if err = wal.Active.WriteRecord(wal.LogRecord{Tombstone: 1, Key: record.Key(), Value: record.Value()}); err != nil {
				errorMsg()
				return false
			}
			mt.Active.Delete(record.Key())
			return true
		} else {
			fmt.Println("Trazeni rekord ne postoji.")
			return false
		}
	}
	errorMsg()
	return false
}

func list() ([]container.DataNode, error) {
	var key string
	fmt.Print("\nUnesite kljuc: ")
	n, err := fmt.Scanf("%s", &key)
	if err != nil || n != 1 {
		return nil, err
	}

	var retVal []container.DataNode

	ret := mt.Active.PrefixScan(key)
	if ret != nil {
		retVal = append(retVal, ret...)
	}

	// If not found in memtable
	// ovde mi upadamo
	//found, ret, err := Finder.PrefixScan([]byte(key))
	found, ret, err := lsmt.Active.PrefixScan([]byte(key))
	if err != nil {
		return nil, err
	} else if !found {
		return retVal, nil
	} else {
		retVal = append(retVal, ret...)
	}
	return retVal, nil
}

func rangeScan() ([]container.DataNode, error) {
	var minKey string
	fmt.Print("\nMinimalni kljuc: ")
	n, err := fmt.Scanf("%s", &minKey)
	if err != nil || n != 1 {
		return nil, err
	}

	var maxKey string
	fmt.Print("Maksimalni kljuc: ")
	n, err = fmt.Scanf("%s", &maxKey)
	if err != nil || n != 1 {
		return nil, err
	}

	if bytes.Compare([]byte(maxKey), []byte(minKey)) <= 0 {
		return nil, fmt.Errorf("minKey > maxKey")
	}

	var retVal []container.DataNode

	ret := mt.Active.RangeScan(minKey, maxKey)
	if ret != nil {
		retVal = append(retVal, ret...)
	}

	//ovde mi upadamo
	//found, ret, err := Finder.RangeScan([]byte(minKey), []byte(maxKey))
	found, ret, err := lsmt.Active.RangeScan([]byte(minKey), []byte(maxKey))
	if err != nil {
		return nil, err
	} else if !found {
		return retVal, nil
	} else {
		retVal = append(retVal, ret...)
	}
	return retVal, nil
}

func main() {
	config.ReadConfig("config.json")

	wal.Init()
	mt.Init()
	lru.Init()
	lsmt.Init()
	////lsmt := LSM.NewLSMTree()
	//lsm.NewLSMTree()
	menu()
}
