package main

import (
	"fmt"
	s "github.com/c-danil0o/NASP/SkipList"
)

func main() {
	skiplist := s.NewSkipList()
	skiplist.Insert(10, []byte("danilo"))
	skiplist.Print()
	skiplist.Insert(23, []byte("danilo2"))
	skiplist.Print()
	skiplist.Insert(2, []byte("danilo3"))
	skiplist.Print()
	skiplist.Insert(1, []byte("danilo3"))
	skiplist.Print()
	skiplist.Insert(2, []byte("danilo4"))
	skiplist.Print()
	skiplist.Insert(3, []byte("danilo3"))
	skiplist.Print()
	skiplist.Insert(4, []byte("danilo4"))
	skiplist.Print()
	skiplist.Insert(14, []byte("danilo4"))
	skiplist.Print()
	skiplist.Delete(14)
	skiplist.Print()
	skiplist.Delete(39)
	skiplist.Print()
	skiplist.Insert(9, []byte("danilo"))
	skiplist.Print()
	skiplist.Insert(33, []byte("danilo2"))
	skiplist.Print()
	skiplist.Insert(21, []byte("danilo3"))
	skiplist.Print()
	skiplist.Insert(7, []byte("danilo4"))
	fmt.Println(skiplist.Find(14))

	skiplist.Print()
}
