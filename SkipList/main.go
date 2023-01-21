package main

import (
	"fmt"
	"math/rand"
)

const (
	MAX_LEVEL = 20
)

type SkipList struct {
	head      *Node
	height    uint
	maxHeight uint
	count     uint
}

type Level struct {
	next *Node
}
type Node struct {
	key    string
	value  []byte
	levels []*Level
}

func NewNode(level uint, key string, value []byte) *Node {
	node := new(Node)
	node.key = key
	node.value = value
	node.levels = make([]*Level, level)
	for i := 0; i < int(level); i++ {
		node.levels[i] = new(Level)
	}
	return node
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:      NewNode(MAX_LEVEL, "0", nil),
		height:    1, //1 najnizi nivo, 2 iznnad
		count:     0,
		maxHeight: MAX_LEVEL,
	}
}
func (s *SkipList) roll() uint {
	var level uint = 0
	for ; rand.Int31n(2) == 1; level++ {
		if level <= s.maxHeight {
			if level > s.height {
				s.height = level
			}
			return level
		}

	}
	return level
}

func (skipl *SkipList) add(key string, value []byte) {
	randomLevel := skipl.roll()
	node := NewNode(randomLevel, key, value)
	levelnodes := make([]*Node, skipl.maxHeight)
	head := skipl.head
	for i := 0; i < int(skipl.height); i++ {
		for head.levels[i].next != nil && head.levels[i].next.key < key {
			head = head.levels[i].next
		}
		if head.levels[i].next != nil && head.levels[i].next.key == key {
			return
		}
		levelnodes[i] = head
	}
	if randomLevel > skipl.height {
		skipl.height = randomLevel
	}
	for i := 0; i < int(randomLevel); i++ {
		if levelnodes[i] == nil {
			skipl.head.levels[i].next = node
		}
		node.levels[i].next = levelnodes[i].levels[i].next
		levelnodes[i].levels[i].next = node
	}
	skipl.count++
	return
}
func (skipl *SkipList) Find(key string) (bool, *Node) {
	var node *Node
	head := skipl.head
	for i := skipl.height - 1; i >= 0; i-- {
		for head.levels[i].next != nil && head.levels[i].next.key <= key {
			head = head.levels[i].next

		}
		if head.key == key {
			node = head
			return true, node
		}
	}
	return false, nil
}

func (sl *SkipList) Delete(key string) bool {
	var node *Node
	last := make([]*Node, sl.height)
	th := sl.head
	// from top to bottom, delete all match nodes
	for i := sl.height - 1; i >= 0; i-- {
		for th.levels[i].next != nil && th.levels[i].next.key < key {
			th = th.levels[i].next
		}

		last[i] = th
		// find the node to delete
		if th.levels[i].next != nil && th.levels[i].next.key == key {
			node = th.levels[i].next
		}
	}

	// no match
	if node == nil {
		return false
	}

	for i := 0; i < len(node.levels); i++ {
		last[i].levels[i].next = node.levels[i].next
		node.levels[i].next = nil
	}

	// delete empty levels
	for i := 0; i < len(sl.head.levels); i++ {
		if sl.head.levels[i].next == nil {
			sl.height = uint(i)
			break
		}
	}
	sl.count--
	return true
}

func (sl *SkipList) IsEmpty() bool {
	return sl.count == 0
}

func (sl *SkipList) Count() uint {
	return sl.count
}
func main() {
	skip := NewSkipList()
	skip.add("danilo", []byte("d"))
	skip.add("danilo2", []byte("dl"))
	skip.add("danilo3", []byte("da"))
	skip.add("danilo4", []byte("ds"))
	skip.add("danilo5", []byte("dd"))
	fmt.Println(skip.Find("danilo"))

}
