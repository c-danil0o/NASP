package main

import (
	"crypto/sha1"
	"encoding/hex"
)

type block string

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

type data interface {
	hash() [20]byte
}
type MerkleTree struct {
	root *Node
}

func (mr *MerkleTree) String() string {
	return mr.root.String()
}
func (mr *MerkleTree) buildMerkle(input []data) Node {
	return Node{}
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func (d block) hash() [20]byte {
	return Hash([]byte(d)[:])
}

func (n *Node) hash() [20]byte {
	var left, right [sha1.Size]byte
	left = n.left.hash()
	right = n.right.hash()
	return Hash(append(left[:], right[:]...))
}

type Empty struct {
}

func (_ Empty) hash() [20]byte {
	return [20]byte{}
}

func main() {

}
