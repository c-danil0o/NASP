package container

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	MAX_LEVEL      = 10
	MAX_KEY_LENGTH = 10
	MIN_KEY        = "0"
	MAX_KEY        = "~~~~~~~~~~"
	//MIN_KEY        = math.MinInt32
	//MAX_KEY        = math.MaxInt32
)

// SkipNode implements DataNode interface
type SkipNode struct {
	key       []byte
	value     []byte
	timestamp int64
	tombstone byte
	forward   []*SkipNode
}

func (node *SkipNode) Value() []byte {
	return node.value
}
func (node *SkipNode) Key() []byte {
	return node.key
}
func (node *SkipNode) Tombstone() byte {
	return node.tombstone
}
func (node *SkipNode) Timestamp() int64 {
	return node.timestamp
}
func (node *SkipNode) SetKey(key []byte) {
	node.key = key
}
func (node *SkipNode) SetValue(value []byte) {
	node.value = value
}
func (node *SkipNode) SetTimestamp(timestamp int64) {
	node.timestamp = timestamp
}
func (node *SkipNode) SetTombstone(tombstone byte) {
	node.tombstone = tombstone
}

type SkipList struct {
	head     *SkipNode
	last     *SkipNode
	maxLevel uint
	height   int
	size     int
}

func newNode(key []byte, value []byte, timestamp int64, tombstone byte, level int) *SkipNode {
	node := new(SkipNode)
	node.key = key
	node.value = value
	node.tombstone = tombstone
	node.timestamp = timestamp
	node.forward = make([]*SkipNode, level)
	for i := 0; i < level; i++ {
		node.forward[i] = nil
	}
	return node
}

func NewSkipList() *SkipList {
	head := newNode([]byte(MIN_KEY), []byte(""), 0, 0, MAX_LEVEL)
	last := newNode([]byte(MAX_KEY), []byte(""), 0, 0, MAX_LEVEL)
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

func (s *SkipList) Find(key []byte) DataNode {
	x := s.head
	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) == -1 {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if bytes.Compare(x.key, key) == 0 {
		return x
	} else {
		return nil
	}
}

func (s *SkipList) Insert(key []byte, value []byte, timestamp int64, tombstone byte) {
	temp := s.Find(key)
	if temp != nil {
		temp.SetValue(value)
		return
	}
	update := make([]*SkipNode, len(s.head.forward))
	copy(update, s.head.forward)
	x := s.head

	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) == -1 {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	var newLevel int = math.MaxInt32
	if bytes.Compare(x.key, key) != 0 {
		for newLevel > MAX_LEVEL {
			newLevel = int(s.roll())
		}
		if newLevel-1 > s.height {
			for i := s.height + 1; i < newLevel; i++ {
				update[i] = s.head
			}
			s.height = newLevel - 1
		}
		x = newNode(key, value, timestamp, tombstone, newLevel)
	}
	for i := 0; i < newLevel; i++ {
		x.forward[i] = update[i].forward[i]
		update[i].forward[i] = x
	}
	s.size++
}

func (s *SkipList) Delete(key []byte) {
	update := make([]*SkipNode, len(s.head.forward))
	copy(update, s.head.forward)
	x := s.head

	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) == -1 {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if bytes.Compare(x.key, key) == 0 {
		for i := 0; i < int(s.maxLevel); i++ {
			if update[i].forward[i] != x {
				break
			}
			//update[i].forward[i] = x.forward[i]
			update[i].forward[i].tombstone = 1
		}
		//s.size--
	}
}

func (s *SkipList) DeleteF(key []byte) {
	update := make([]*SkipNode, len(s.head.forward))
	copy(update, s.head.forward)
	x := s.head

	for i := s.height; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) == -1 {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if bytes.Compare(x.key, key) == 0 {
		for i := 0; i < int(s.maxLevel); i++ {
			if update[i].forward[i] != x {
				break
			}
			update[i].forward[i] = x.forward[i]
		}
		x = nil
		s.size--
	}
}
func (s *SkipList) Size() int {
	return s.size
}
func (s *SkipList) Print() {
	var node *SkipNode
	for i := int(s.maxLevel - 1); i >= 0; i-- {
		node = s.head.forward[i]
		fmt.Printf("Level: %v ---> ", i)
		for node != nil && bytes.Compare(node.key, []byte(MAX_KEY)) != 0 {
			fmt.Printf(" %v %v,", string(node.key), node.tombstone)
			node = node.forward[i]
		}
		fmt.Println()
	}
	fmt.Println()
}

func (s *SkipList) GetSortedData() []DataNode {
	var node = s.head.forward[0]
	var result []DataNode
	for node != nil && bytes.Compare(node.key, []byte(MAX_KEY)) != 0 {
		result = append(result, node)
		node = node.forward[0]
	}
	return result
}
