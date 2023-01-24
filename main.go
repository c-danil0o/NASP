package main

import (
	mt "github.com/c-danil0o/NASP/Memtable"
)

func main() {

	result := mt.ReadFile("config.json")

	kapacitet := result["capacity"]
	brSeg := result["num_of_segments"]
	treshold := result["trashold"]

	memTable := mt.CreateMemtable(kapacitet, brSeg, treshold)
	memTable.Add(mt.Element{key: 12, value: []byte("danilo")})
}
