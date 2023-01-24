package main

import (
	mt "github.com/c-danil0o/NASP/Memtable"
)

func main() {

	result := mt.ReadFile("config.json")

	kapacitet := result["capacity"]
	brSeg := result["num_of_segments"]
	treshold := result["trashold"]

	mt.Init(kapacitet, brSeg, treshold)
	mt.Active.Add(mt.Element{Key: []byte("penis"), Value: []byte("danilo")})
	mt.Active.Add(mt.Element{Key: []byte("penis1"), Value: []byte("danilo")})
	mt.Active.Add(mt.Element{Key: []byte("penia"), Value: []byte("danilo")})
	mt.Active.Add(mt.Element{Key: []byte("penib"), Value: []byte("danilo")})
	mt.Active.Add(mt.Element{Key: []byte("penic"), Value: []byte("danilo")})
	mt.Active.Add(mt.Element{Key: []byte("penid"), Value: []byte("danilo")})
	mt.Active.Print()
	mt.CheckThreshold()
}
