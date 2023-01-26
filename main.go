package main

import (
	mt "github.com/c-danil0o/NASP/Memtable"
)

func main() {

	result := mt.ReadFile("config.json")

	kapacitet := result["memtable_capacity"]
	brSeg := result["memtable_num_of_segments"]
	treshold := result["memtable_threshold"]
	mt.Init(kapacitet, brSeg, treshold)
	mt.CheckThreshold()
	mt.Active.Add(mt.Element{Key: []byte("abcdea"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdeb"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdec"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcded"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdee"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdef"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdeg"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdeh"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdei"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdej"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdek"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdel"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdem"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcden"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Add(mt.Element{Key: []byte("abcdeo"), Value: []byte("danilo")})
	mt.CheckThreshold()

	mt.Active.Print()
	//mt.CheckThreshold()
}
