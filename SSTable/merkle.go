package SSTable

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type MerkleRoot struct {
	Root *Node
}

type Node struct {
	//data     interface{} // /vrednosti njega samog ako je list

	hashData [20]byte //hes kljuc vrednosti ispod njega
	data     Record
	left     *Node
	right    *Node
	leaf     bool //bice true akko je cvor poslednji tj list
}

func GenerateMerkle(dataSlice []Record) *MerkleRoot {
	//var dataSlice = []interface{}{1, 2, 3, 4, 5, 6, 7} //ovo ce biti prosledjeni parametar funkciji samo da vidimo

	var depth int = 1                 //broji kolika je visina stabla
	var noOfElements = len(dataSlice) //sa kojim tipovima podataka cemo raditi
	var index int = 0
	var indexOfElement *int = &index

	mr := MerkleRoot{&Node{left: nil, right: nil, leaf: false}}
	mr.Root.left = generateNodes(noOfElements, depth+1, indexOfElement, dataSlice)
	mr.Root.right = generateNodes(noOfElements, depth+1, indexOfElement, dataSlice)
	mr.Root.hashData = hashHashed(mr.Root.left.hashData, mr.Root.right.hashData)

	return &mr
}

func generateNodes(noOfElements int, depth int, indexOfElement *int, dataSlice []Record) *Node {

	spaceCovered := math.Pow(2, float64(depth))
	var n Node = Node{left: nil, right: nil, leaf: false}

	if spaceCovered >= float64(noOfElements) {

		var newLeftNode Node = Node{left: nil, right: nil, leaf: true}
		if len(dataSlice) > *indexOfElement {
			newLeftNode.data = dataSlice[*indexOfElement]
			*indexOfElement += 1
			byteData := []byte(fmt.Sprintf("%+v", newLeftNode.data))
			newLeftNode.hashData = hash(byteData)
		}
		n.left = &newLeftNode

		var newRightNode Node = Node{left: nil, right: nil, leaf: true}
		if len(dataSlice) > *indexOfElement {

			newRightNode.data = dataSlice[*indexOfElement]
			*indexOfElement += 1
			byteData := []byte(fmt.Sprintf("%+v", newRightNode.data))
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

func (mr *MerkleRoot) SerializeMerkle(writer io.Writer) error {
	var buf bytes.Buffer
	bufPT := &buf
	err := SerializeMerkleNodes(writer, bufPT, mr.Root)
	_, err = writer.Write(buf.Bytes())
	return err
}

func SerializeMerkleNodes(writer io.Writer, buf *bytes.Buffer, parentNode *Node) error {

	err := binary.Write(buf, binary.BigEndian, parentNode.hashData)
	err = binary.Write(buf, binary.BigEndian, parentNode.leaf)
	// upisivanje oznake da li je njegovo dete list
	err = binary.Write(buf, binary.BigEndian, parentNode.left.leaf)

	//videti kad se bude radila deserijalizacija
	//da li je bolje da ne-listovi budu zapisani sa samo gornja 3 parametra

	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.CRC)
	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.Timestamp)
	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.Tombstone)
	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.KeySize)
	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.ValueSize)
	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.Key)
	//err = binary.Write(&buf, binary.BigEndian, parentNode.data.Value)

	// ovde treba izmeniti ovaj sprintf ne moze ili neki drugi nacin

	if parentNode.left.leaf == true {

		//upisivanje levog lista
		err = binary.Write(buf, binary.BigEndian, parentNode.left.hashData)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.leaf)

		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.CRC)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.timestamp)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.tombstone)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.KeySize)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.ValueSize)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.key)
		err = binary.Write(buf, binary.BigEndian, parentNode.left.data.value)

		//upisivanje desnog lista
		err = binary.Write(buf, binary.BigEndian, parentNode.right.hashData)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.leaf)

		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.CRC)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.timestamp)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.tombstone)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.KeySize)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.ValueSize)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.key)
		err = binary.Write(buf, binary.BigEndian, parentNode.right.data.value)

		return err
	} else {
		//ovde se poziva rekurzija kojom se ide u dubinu dok se ne stigne do listova
		err = SerializeMerkleNodes(writer, buf, parentNode.left)
		err = SerializeMerkleNodes(writer, buf, parentNode.right)
	}

	return err
}
