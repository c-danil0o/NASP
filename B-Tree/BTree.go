package BTree

import (
	"bytes"
	"fmt"
)

type Element struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Tombstone byte
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
			keys:     make([]*Element, 0),
			children: make([]*Node, 0),
			leaf:     true,
		},
		T: T,
	}
}

func (tree *BTree) Insert(e *Element) {
	r := tree.root
	if len(tree.root.keys) == 2*tree.T-1 {
		temp := &Node{
			keys:     make([]*Element, 0),
			children: make([]*Node, 0),
			leaf:     false,
		}
		tree.root = temp
		temp.children = append([]*Node{r}, temp.children...)
		tree.splitChild(temp, 0)
		tree.insertNonFull(temp, e)
	} else {
		tree.insertNonFull(r, e)
	}
}

func (tree *BTree) insertNonFull(x *Node, e *Element) {
	i := len(x.keys) - 1
	if x.leaf {
		for (i >= 0) && (bytes.Compare(e.Key, x.keys[i].Key) == -1) {
			if i+1 >= len(x.keys) {
				x.keys = append(x.keys, x.keys[i])
			} else {
				x.keys[i+1] = x.keys[i]
			}
			i--
		}

		x.keys = append(x.keys, nil)
		copy(x.keys[i+2:], x.keys[i+1:])
		x.keys[i+1] = e
	} else {
		for (i >= 0) && (bytes.Compare(e.Key, x.keys[i].Key) == -1) {
			i--
		}
		i++
		if len(x.children[i].keys) == 2*tree.T-1 {
			tree.splitChild(x, i)
			if bytes.Compare(e.Key, x.keys[i].Key) == 1 {
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
		keys:     make([]*Element, tree.T-1),
		children: make([]*Node, 0),
		leaf:     y.leaf,
	}

	x.children = append(x.children[:i+1], append([]*Node{z}, x.children[i+1:]...)...)
	x.keys = append(x.keys[:i], append([]*Element{y.keys[tree.T-1]}, x.keys[i:]...)...)
	z.keys = y.keys[tree.T : (2*tree.T)-1]
	y.keys = y.keys[0 : tree.T-1]
	if !y.leaf {
		z.children = y.children[tree.T : 2*tree.T]
		y.children = y.children[0:tree.T]
	}
}

func (t *BTree) PrintTree(x *Node, l int) {
	fmt.Printf("Level %d %d:", l, len(x.keys))
	for _, i := range x.keys {
		fmt.Print(string(i.Key), " ")
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

/*func main() {
	stablo := CreateBTree(2)

	stablo.Insert(&Element{
		Key:       []byte("2"),
		Value:     []byte("2"),
		Timestamp: 14,
		Tombstone: 0,
	})

	stablo.PrintTree(stablo.root, 0)
	fmt.Print("\n")

	stablo.Insert(&Element{
		Key:       []byte("3"),
		Value:     []byte("3"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("4"),
		Value:     []byte("4"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("5"),
		Value:     []byte("5"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("6"),
		Value:     []byte("6"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("7"),
		Value:     []byte("7"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("8"),
		Value:     []byte("8"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("9"),
		Value:     []byte("9"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

	stablo.Insert(&Element{
		Key:       []byte("10"),
		Value:     []byte("10"),
		Timestamp: 14,
		Tombstone: 0,
	})
	fmt.Print("\n")
	stablo.PrintTree(stablo.root, 0)

}
*/
