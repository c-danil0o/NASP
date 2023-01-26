package merkle

import (
	"crypto/sha1"
	"fmt"
	"math"
)

type MerkleRoot struct {
	root *Node
}

type Node struct {
	hashData [20]byte    //hes kljuc vrednosti ispod njega
	data     interface{} // /vrednosti njega samog ako je list
	left     *Node
	right    *Node
	leaf     bool //bice true akko je cvor poslednji tj list
}

func GenerateMerkle() *MerkleRoot {
	var depth int = 1                                  //broji kolika je visina stabla
	var dataSlice = []interface{}{1, 2, 3, 4, 5, 6, 7} //ovo ce biti prosledjeni parametar funkciji samo da vidimo
	var noOfElements = len(dataSlice)                  //sa kojim tipovima podataka cemo raditi
	var index int = 0
	var indexOfElement *int = &index
	mr := MerkleRoot{&Node{left: nil, right: nil, data: nil, leaf: false}}
	mr.root.left = generateNodes(noOfElements, depth+1, indexOfElement, dataSlice)
	mr.root.right = generateNodes(noOfElements, depth+1, indexOfElement, dataSlice)
	mr.root.hashData = hashHashed(mr.root.left.hashData, mr.root.right.hashData)
	return &mr
}

func generateNodes(noOfElements int, depth int, indexOfElement *int, dataSlice []interface{}) *Node {
	spaceCovered := math.Pow(2, float64(depth))
	var n Node = Node{left: nil, right: nil, data: nil, leaf: false}
	if spaceCovered >= float64(noOfElements) {
		var newLeftNode Node = Node{left: nil, right: nil, data: nil, leaf: true}
		if len(dataSlice) > *indexOfElement {
			newLeftNode.data = dataSlice[*indexOfElement]
			*indexOfElement += 1
			byteData := []byte(fmt.Sprintf("%v", newLeftNode.data.(interface{})))
			newLeftNode.hashData = hash(byteData)
		}
		n.left = &newLeftNode
		var newRightNode Node = Node{left: nil, right: nil, data: nil, leaf: true}
		if len(dataSlice) > *indexOfElement {
			newRightNode.data = dataSlice[*indexOfElement]
			*indexOfElement += 1
			byteData := []byte(fmt.Sprintf("%v", newRightNode.data.(interface{})))
			newRightNode.hashData = hash(byteData)
		}
		n.right = &newRightNode
		n.hashData = hashHashed(n.left.hashData, n.right.hashData)

	} else {
		var newLeftNode = generateNodes(noOfElements, depth+1, indexOfElement, dataSlice)
		n.left = newLeftNode
		var newRightNode = generateNodes(noOfElements, depth+1, indexOfElement, dataSlice)
		n.right = newRightNode
		n.hashData = hashHashed(n.left.hashData, n.right.hashData)

	}
	return &n

}

func hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func hashHashed(dataL [20]byte, dataR [20]byte) [20]byte {
	data := append(dataL[:], dataR[:]...)
	return sha1.Sum(data)
}
