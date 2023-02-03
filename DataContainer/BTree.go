package container

import (
	"bytes"
	"fmt"
)

type Element struct {
	key       []byte
	value     []byte
	timestamp int64
	tombstone byte
}

type Node struct {
	keys     []*Element
	children []*Node
	leaf     bool
}

type BTree struct {
	root *Node
	T    int
}

func CreateBTree(T int) *BTree {
	return &BTree{
		root: &Node{
			keys:     make([]*Element, 0, 2*T-1),
			children: make([]*Node, 0, 2*T),
			leaf:     true,
		},
		T: T,
	}
}

func (e *Element) Value() []byte {
	return e.value
}
func (e *Element) Key() []byte {
	return e.key
}
func (e *Element) Tombstone() byte {
	return e.tombstone
}
func (e *Element) Timestamp() int64 {
	return e.timestamp
}
func (e *Element) SetKey(key []byte) {
	e.key = key
}
func (e *Element) SetValue(value []byte) {
	e.value = value
}
func (e *Element) SetTimestamp(timestamp int64) {
	e.timestamp = timestamp
}
func (e *Element) SetTombstone(tombstone byte) {
	e.tombstone = tombstone
}

func (tree *BTree) search(key []byte, temp *Node) DataNode {
	i := 0
	for i < len(temp.keys) && bytes.Compare(key, temp.keys[i].key) == 1 {
		i++
	}
	if i < len(temp.keys) && bytes.Compare(key, temp.keys[i].key) == 0 {
		return temp.keys[i]
	}
	if temp.leaf {
		return nil
	}

	return tree.search(key, temp.children[i])
}

func (tree *BTree) Find(key []byte) DataNode {
	return tree.search(key, tree.root)
}

func (tree *BTree) Insert(key []byte, value []byte, timestamp int64, tombstone byte) {
	r := tree.root

	e := &Element{
		key:       key,
		value:     value,
		timestamp: timestamp,
		tombstone: tombstone,
	}

	x := tree.Find(e.key)

	if x != nil {
		x.SetValue(e.value)
		x.SetTimestamp(e.timestamp)
		x.SetTombstone(e.tombstone)
		return
	}

	if len(tree.root.keys) == 2*tree.T-1 {
		temp := &Node{
			keys:     make([]*Element, 0, 2*tree.T-1),
			children: make([]*Node, 0, 2*tree.T),
			leaf:     false,
		}
		tree.root = temp
		temp.children = append(temp.children, r)
		tree.splitChild(temp, 0)
		tree.insertNonFull(temp, e)
	} else {
		tree.insertNonFull(r, e)
	}
}

func (tree *BTree) insertNonFull(x *Node, e *Element) {
	i := len(x.keys) - 1
	if x.leaf {
		for (i >= 0) && (bytes.Compare(e.key, x.keys[i].key) == -1) {
			i--
		}

		x.keys = append(x.keys, nil)
		copy(x.keys[i+2:], x.keys[i+1:])
		x.keys[i+1] = e
	} else {
		for (i >= 0) && (bytes.Compare(e.key, x.keys[i].key) == -1) {
			i--
		}
		i++
		if len(x.children[i].keys) == 2*tree.T-1 {
			tree.splitChild(x, i)
			if bytes.Compare(e.key, x.keys[i].key) == 1 {
				i++
			}
			tree.insertNonFull(x.children[i], e)
		} else {
			tree.insertNonFull(x.children[i], e)
		}

	}
}

func (tree *BTree) splitChild(x *Node, i int) {
	y := x.children[i]
	z := &Node{
		leaf:     y.leaf,
		keys:     *new([]*Element),
		children: *new([]*Node),
	}

	z.keys = append(z.keys, y.keys[tree.T:(2*tree.T)-1]...)

	if !y.leaf {
		z.children = append(z.children, y.children[tree.T:2*tree.T]...)
		var temp []*Node
		temp = append(temp, y.children[0:tree.T]...)
		y.children = make([]*Node, 0)
		y.children = append(y.children, temp...)
	}

	x.children = append(x.children[:i+1], append([]*Node{z}, x.children[i+1:]...)...)
	x.keys = append(x.keys[:i], append([]*Element{y.keys[tree.T-1]}, x.keys[i:]...)...)

	y.keys = y.keys[0 : tree.T-1]

}

func (tree *BTree) traverse(n *Node, num *int) {
	for _, k := range n.keys {
		if k.tombstone == 0 {
			*num++
		}
	}

	for i := 0; i < len(n.children); i++ {
		if !n.leaf {
			tree.traverse(n.children[i], num)
		}
	}
}

func (tree *BTree) Print() {
	tree.PrintTree(tree.root, 0)
}

func (t *BTree) PrintTree(x *Node, l int) {
	fmt.Printf("Level %d %d:", l, len(x.keys))
	for _, i := range x.keys {
		fmt.Print(string(i.value), " ")
	}
	fmt.Println()
	l += 1
	if len(x.children) > 0 {
		for _, i := range x.children {
			if i != nil {
				t.PrintTree(i, l)
			}
		}
	}
}

func (tree *BTree) Size() int {
	num := 0
	tree.traverse(tree.root, &num)
	return num
}

func (tree *BTree) Delete(key []byte) {
	temp := tree.Find(key)

	if temp != nil {
		temp.SetTombstone(1)
	}

}

func (tree *BTree) dataTraverse(n *Node, data *[]Element) {
	if n.leaf {
		for _, k := range n.keys {
			if k.tombstone == 0 {
				*data = append(*data, *k)
			}
		}
	} else {
		for i := 0; i < len(n.keys); i++ {
			tree.dataTraverse(n.children[i], data)
			if n.keys[i].tombstone == 0 {
				*data = append(*data, *n.keys[i])
			}
		}
		tree.dataTraverse(n.children[len(n.keys)], data)
	}
}

func (tree *BTree) GetSortedData() []Element {
	var data []Element

	if tree.root == nil {
		return data
	}
	tree.dataTraverse(tree.root, &data)

	return data
}
