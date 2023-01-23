package skiplist

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	MAX_LEVEL = 10
	MIN_KEY   = math.MinInt32
	MAX_KEY   = math.MaxInt32
)

type SkipNode struct {
	key     int
	value   []byte
	forward []*SkipNode
}

type SkipList struct {
	head     *SkipNode
	last     *SkipNode
	maxLevel uint
	height   int
	size     uint
}

func newNode(key int, value []byte, level int) *SkipNode {
	node := new(SkipNode)
	node.key = key
	node.value = value
	node.forward = make([]*SkipNode, level)
	for i := 0; i < level; i++ {
		node.forward[i] = nil
	}
	return node
}

func NewSkipList() *SkipList {
	head := newNode(MIN_KEY, []byte(""), MAX_LEVEL)
	last := newNode(MAX_KEY, []byte(""), MAX_LEVEL)
	for i := 0; i < len(head.forward); i++ {
		head.forward[i] = last
	}
	return &SkipList{
		maxLevel: MAX_LEVEL,
		head:     head,
		last:     last,
		height:   0,
		size:     0,
	}
}
func (s *SkipList) roll() uint {
	var level uint = 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for r.Int31n(2) == 1 {
		level++
		if level >= s.maxLevel {
			return level
		}
	}
	return level
}

func (s *SkipList) Find(key int) *SkipNode {
	x := s.head
	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x.key == key {
		return x
	} else {
		return nil
	}
}

func (s *SkipList) Insert(key int, value []byte) {
	temp := s.Find(key)
	if temp != nil {
		temp.value = value
		return
	}
	update := make([]*SkipNode, len(s.head.forward))
	copy(update, s.head.forward)
	x := s.head

	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	var newLevel int = MAX_KEY
	if x.key != key {
		for newLevel > MAX_LEVEL {
			newLevel = int(s.roll())
		}
		if newLevel-1 > s.height {
			for i := s.height + 1; i < newLevel; i++ {
				update[i] = s.head
			}
			s.height = newLevel - 1
		}
		x = newNode(key, value, newLevel)
	}
	for i := 0; i < newLevel; i++ {
		x.forward[i] = update[i].forward[i]
		update[i].forward[i] = x
	}
}
func (s *SkipList) Delete(key int) {
	update := make([]*SkipNode, len(s.head.forward))
	copy(update, s.head.forward)
	x := s.head

	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if key == x.key {
		for i := 0; i < int(s.maxLevel); i++ {
			if update[i].forward[i] != x {
				break
			}
			update[i].forward[i] = x.forward[i]
		}
		x = nil
	}

}
func (s *SkipList) Print() {
	var node *SkipNode
	for i := int(s.maxLevel - 1); i >= 0; i-- {
		node = s.head.forward[i]
		fmt.Printf("Level: %v ---> ", i)
		for node != nil && node.key != MAX_KEY {
			fmt.Printf(" %v", node.key)
			node = node.forward[i]
		}
		fmt.Println()
	}
	fmt.Println()
}
