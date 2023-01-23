package main

import (
	"fmt"
	hll "github.com/c-danil0o/NASP/HyperLogLog"
)

func main() {
	bs, _ := hll.GetRandomData()
	h := hll.NewHyperLogLog(16)
	for _, b := range bs {
		h.Add(b)
	}
	hd := h.Estimate()
	fmt.Println(hd)

}
