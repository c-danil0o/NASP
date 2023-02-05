package LSM

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	sst "github.com/c-danil0o/NASP/SSTable"

	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
	"github.com/c-danil0o/NASP/Finder"
)

type LSMTree struct {
	//memTable memtable.Memtable
	max   int
	nodes *LSMNode
}
type LSMNode struct {
	sstG []int
	next *LSMNode
	lvl  int
}

var Active LSMTree

func Init() {
	Active = *NewLSMTree()
	Active.DeserializeLSMT()
	// fmt.Println(Active.GetNextGeneration())
	// memtable.Generation = uint32(Active.GetNextGeneration())
}

// mem memtable.Memtable
func NewLSMTree() *LSMTree {
	return &LSMTree{
		max: config.LSM_DEPTH,
		nodes: &LSMNode{
			sstG: []int{},
			next: nil,
			lvl:  1,
		},
	}
}

// ovde ce morati da se menja logika i to ozbiljno
// treba upariti po dva
// kako kroz rekurziju ???
//func (lsm *LSMTree) insertInNode(SSTable int, node *LSMNode) error {
//	if node.sstG[0] == -1 {
//		node.sstG[0] = SSTable
//	} else {
//		if node.lvl == lsm.max {
//			fmt.Println("Popunjen je max level LSMa.")
//		} else {
//			var novaGen int
//			if node.sstG > SSTable {
//				novaGen = node.sstG + 1
//			} else {
//				novaGen = SSTable + 1
//			}
//			fmt.Println("MRDZ:")
//			fmt.Println(node.sstG)
//			fmt.Println(SSTable)
//			fmt.Println(novaGen)
//			fmt.Println("======")
//			err, temp := sst.Merge(node.sstG, SSTable, novaGen)
//			//os.removefiles(node.sstg)
//			//os.removefiles(sstable)
//			if err != nil {
//				return err
//			}
//
//			if temp >= config.MEMTABLE_THRESHOLD*int(math.Pow(2, float64(node.lvl))) {
//				if node.next == nil {
//					node.next = &LSMNode{
//						sstG: -1,
//						next: nil,
//						lvl:  node.lvl + 1,
//					}
//				}
//				node.sstG = -1
//				return lsm.insertInNode(novaGen, node.next)
//			} else {
//				node.sstG = novaGen
//			}
//		}
//	}
//
//	return nil
//}

// err, temp := sst.Merge(node.sstG, SSTable, novaGen)

func (lsmt *LSMTree) Compact() error {
	current := lsmt.nodes
	var err error
	for current != nil {
		var size int
		j := 1
		temp := -1
		for i := 0; i < len(current.sstG); i += 2 {
			newGen := lsmt.GetNextGeneration()
			if j < len(current.sstG) && i < len(current.sstG) {
				if temp == -1 { //spaja dva nova prvi mrdz / prosli mrdz je gurnuo dole
					err, size = sst.Merge(current.sstG[i], current.sstG[j], newGen)
					if err != nil {
						return err
					}
				} else { //prethodno spajanje je manje od size pa se spaja sa 1
					err, size = sst.Merge(current.sstG[temp], current.sstG[i], newGen)
					if err != nil {
						return err
					}
					i--
				}
				if size > config.MEMTABLE_THRESHOLD*int(math.Pow(2, float64(current.lvl))) {
					if current.next != nil {
						current.next.sstG = append(current.next.sstG, newGen)
					} else { // gura na nizi nivo u stablu
						if current.lvl >= config.LSM_DEPTH {
							println("Doslo je do gornje granice stabla, nema vise mesta!")
						} else {
							current.next = &LSMNode{sstG: []int{newGen}, next: nil, lvl: current.lvl + 1}
						}
					}
					temp = -1
				} else {
					temp = i
				}
				j += 2
			}

		}
		if temp == -1 {
			current.sstG = []int{}
		} else {
			current.sstG = []int{temp}
		}
		if current.lvl >= config.LSM_DEPTH {
			fmt.Println("Vase stablo je puno nema vise mesta!Kompakcija je obustavljena.")
			break
		}
		current = current.next
	}
	return nil
}

func (lsmt *LSMTree) Compaction() error {
	current := lsmt.nodes
	//var err error = nil
	var newGen int
	var endOFLevel bool = false
	for current != nil {
		size := 0
		i := 0
		j := 1
		for !endOFLevel {
			for size < config.MEMTABLE_THRESHOLD*int(math.Pow(2, float64(current.lvl))) {
				if j < len(current.sstG) || i < len(current.sstG) {
					newGen = lsmt.GetNextGeneration()
					err, temp := sst.Merge(current.sstG[i], current.sstG[j], newGen)
					if err != nil {
						return err
					}
					size = temp
					i++
					j++

				} else {
					endOFLevel = true
					break
				}
			}
			if size >= config.MEMTABLE_THRESHOLD*int(math.Pow(2, float64(current.lvl))) {
				if current.next != nil {
					current.next.sstG = append(current.next.sstG, newGen)
				} else {
					current.next = &LSMNode{sstG: []int{newGen}, next: nil, lvl: current.lvl + 1}
					//current.next.sstG = append(current.next.sstG, )
				}
			}
		}

		current = current.next

	}
	return nil
}

func (lsm *LSMTree) insertInNode(sst int, node *LSMNode) error {
	lsm.nodes.sstG = append(lsm.nodes.sstG, sst)
	fmt.Println(node.sstG)
	return nil
}

func (lsm *LSMTree) InsertSST(sst int) error {
	return lsm.insertInNode(sst, lsm.nodes)
}

func (lsm *LSMTree) FindKey(key []byte) (bool, container.DataNode, error) {
	var found bool
	var err error
	var retVal container.DataNode
	current := lsm.nodes
	for current != nil {
		for i := len(current.sstG) - 1; i >= 0; i-- {
			if current.sstG[i] != -1 {
				found, retVal, err = Finder.FindKey(key, uint32(current.sstG[i]))
				if found {
					return found, retVal, err
				}
			}
		}
		current = current.next
	}
	//found, retVal, err = Finder.FindKey(key, uint32(current.sstG))
	return found, retVal, err
}

func (lsm *LSMTree) PrefixScan(key []byte) (bool, []container.DataNode, error) {
	var found bool
	var err error
	var retVal []container.DataNode
	var tempRetVal []container.DataNode
	current := lsm.nodes
	foundVals := make(map[string]container.DataNode)
	for current != nil {
		for i := len(current.sstG) - 1; i >= 0; i-- {
			if current.sstG[i] != -1 {
				found, tempRetVal, err = Finder.PrefixScan(key, uint32(current.sstG[i]))
				if found {
					for _, v := range tempRetVal {
						_, ok := foundVals[string(v.Key())]
						if !ok {
							foundVals[string(v.Key())] = v
						}
					}
				}
			}
		}
		current = current.next
	}
	for _, k := range foundVals {
		retVal = append(retVal, k)
	}
	return found, retVal, err
}

func (lsm *LSMTree) RangeScan(minKey []byte, maxKey []byte) (bool, []container.DataNode, error) {
	var found bool
	var err error
	var retVal []container.DataNode
	var tempRetVal []container.DataNode
	current := lsm.nodes
	foundVals := make(map[string]container.DataNode)
	for current != nil {
		for i := len(current.sstG) - 1; i >= 0; i-- {
			if current.sstG[i] != -1 {
				found, tempRetVal, err = Finder.RangeScan(minKey, maxKey, uint32(current.sstG[i]))
				if found {
					for _, v := range tempRetVal {
						_, ok := foundVals[string(v.Key())]
						if !ok {
							foundVals[string(v.Key())] = v
						}
					}
				}
			}
		}
		current = current.next
	}
	for _, k := range foundVals {
		retVal = append(retVal, k)
	}
	return found, retVal, err
}

func RemoveFiles(generation int32) error {

	err := os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Data.db")
	if err != nil {
		return err
	}
	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Index.db")
	if err != nil {
		return err
	}
	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Summary.db")
	if err != nil {
		return err
	}
	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Filter.db")
	if err != nil {
		return err
	}
	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-TOC.txt")
	if err != nil {
		return err
	}
	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Metadata.txt")
	if err != nil {
		return err
	}
	return nil

}

func (lsmt *LSMTree) Serialize() error {
	lsmtreeFile, err := os.OpenFile("LSMTree.bin", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	defer lsmtreeFile.Close()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	current := lsmt.nodes
	for current != nil {
		err = binary.Write(&buf, binary.BigEndian, int64(len(current.sstG))) //duzina generacija
		for i := 0; i < len(current.sstG); i++ {
			err = binary.Write(&buf, binary.BigEndian, int64(current.sstG[i])) //svaki gen u cvoru
		}
		err = binary.Write(&buf, binary.BigEndian, int64(current.lvl)) // level

		current = current.next
	}
	_, err = lsmtreeFile.Write(buf.Bytes())
	lsmtreeFile.Sync()

	return err
}

func (lsmt *LSMTree) Serialize1() error {
	binFile, err := os.Create("LSMTree.bin")
	if err != nil {
		return err
	}
	defer binFile.Close()
	current := lsmt.nodes
	for current != nil {
		//fmt.Println(current.sstG)
		err = binary.Write(binFile, binary.BigEndian, int64(current.sstG[0]))
		if err != nil {
			return err
		}
		//fmt.Println(current.lvl)
		err = binary.Write(binFile, binary.BigEndian, int64(current.lvl))
		if err != nil {
			return err
		}
		current = current.next
	}

	return nil
}

func (lsmt *LSMTree) DeserializeLSMT() error {

	lsmtreeFile, err := os.OpenFile("LSMTree.bin", os.O_RDONLY, 0600)
	//lsmt := LSMTree{max: config.LSM_DEPTH, nodes: nil}
	if err != nil {
		return err
	}
	defer lsmtreeFile.Close()

	lsmt.nodes = &LSMNode{sstG: []int{}, lvl: 0, next: nil}
	current := lsmt.nodes

	//var isize int64
	var data int64
	for true {
		err = binary.Read(lsmtreeFile, binary.BigEndian, &data)
		if err == io.EOF {
			break
		}
		if err == nil {
			for i := int64(0); i < data; i++ {
				err = binary.Read(lsmtreeFile, binary.BigEndian, &data)
				if err == nil {
					current.sstG = append(current.sstG, int(data))
				}
			}
			err = binary.Read(lsmtreeFile, binary.BigEndian, &data)
			if err == nil {
				current.lvl = int(data)
			}
		}
		current.next = &LSMNode{sstG: []int{-1}, lvl: current.lvl + 1, next: nil}
		current = current.next
	}
	return err
}

//######################################################
//	for true {
//
//		err = binary.Read(lsmtreeFile, binary.BigEndian, &data)
//		if err == io.EOF {
//			break
//		}
//		if err == nil {
//			current.sstG = int(data)
//		}
//
//		err = binary.Read(lsmtreeFile, binary.BigEndian, &data)
//		if err == nil {
//			current.lvl = int(data)
//		}
//		current.next = &LSMNode{sstG: []int{-1}, lvl: current.lvl + 1, next: nil}
//		current = current.next
//	}
//	return err
//}

//func (lsmt *LSMTree) DeserializeLSMT1() error {
//	binFile, err := os.Open("LSMTree.bin")
//	if err != nil {
//		return err
//	}
//	defer binFile.Close()
//
//	lsmt.nodes = &LSMNode{sstG: -1, lvl: 0, next: nil}
//	current := lsmt.nodes
//
//	var temp1, temp2 int64
//
//	for {
//		err = binary.Read(binFile, binary.BigEndian, &temp1)
//		if err != nil {
//			if err == io.EOF {
//				break
//			} else {
//				return err
//			}
//		}
//
//		err = binary.Read(binFile, binary.BigEndian, &temp2)
//		if err != nil {
//			if err == io.EOF {
//				break
//			} else {
//				return err
//			}
//		}
//		//fmt.Println("PRVI: " + string(int(temp1)))
//		//fmt.Print("DRUGI: " + string(int(temp2)))
//		current.sstG = int(temp1)
//		current.lvl = int(temp2)
//		current.next = &LSMNode{sstG: -1, lvl: current.lvl + 1, next: nil}
//		current = current.next
//	}
//
//	return nil
//}

func (lsmt *LSMTree) GetNextGeneration() int {
	var gen int = -1
	current := lsmt.nodes
	for current != nil {
		for i := len(current.sstG) - 1; i >= 0; i-- {
			if gen < current.sstG[i] {
				gen = current.sstG[i]
			}
		}
		current = current.next
	}
	if gen == -1 {
		return 0
	} else {
		return gen + 1
	}
}

//func (lsmt *LSMTree) GetNextGeneration() int {
//	var gen int = -1
//	current := lsmt.nodes
//	for current != nil {
//		if len(current.sstG) > 0 {
//			ind := len(current.sstG) - 1
//			gen = current.sstG[ind]
//		}
//		current = current.next
//	}
//}
//package LSM
//
//import (
//<<<<<<< HEAD
//	"bytes"
//	"encoding/binary"
//	"fmt"
//	config "github.com/c-danil0o/NASP/Config"
//	container "github.com/c-danil0o/NASP/DataContainer"
//	"github.com/c-danil0o/NASP/Finder"
//	memtable "github.com/c-danil0o/NASP/Memtable"
//	sst "github.com/c-danil0o/NASP/SSTable"
//	"io"
//	"math"
//	"os"
//	"strconv"
//	"unsafe"
//=======
//	"fmt"
//	"math"
//	"os"
//	"strconv"
//
//	config "github.com/c-danil0o/NASP/Config"
//	container "github.com/c-danil0o/NASP/DataContainer"
//	"github.com/c-danil0o/NASP/Finder"
//	sst "github.com/c-danil0o/NASP/SSTable"
//	//memtable "github.com/c-danil0o/NASP/Memtable"
//	//memtable "github.com/c-danil0o/NASP/Memtable"
//>>>>>>> 74927af1c413fc2d6b93c63f2d2bc5f5adc76379
//)
//
//type LSMTree struct {
//	//memTable memtable.Memtable
//	max   int
//	nodes *LSMNode
//}
//type LSMNode struct {
//	sstG int
//	next *LSMNode
//	lvl  int
//}
//
//var Active LSMTree
//
//func Init() {
//	Active = *NewLSMTree()
//	Active.DeserializeLSMT()
//	memtable.Generation = uint32(Active.GetNextGeneration())
//}
//
//// mem memtable.Memtable
//func NewLSMTree() *LSMTree {
//	return &LSMTree{
//		max: config.LSM_DEPTH,
//		nodes: &LSMNode{
//			sstG: -1,
//			next: nil,
//			lvl:  1,
//		},
//	}
//}
//
//func (lsm *LSMTree) insertInNode(SSTable int, node *LSMNode) error {
//	if node.sstG == -1 {
//		node.sstG = SSTable
//	} else {
//		if node.lvl == lsm.max {
//			fmt.Println("Popunjen je max level LSMa.")
//		} else {
//			var novaGen int
//			if node.sstG > SSTable {
//				novaGen = node.sstG + 1
//			} else {
//				novaGen = SSTable + 1
//			}
//<<<<<<< HEAD
//
//			err, temp := sst.Merge(node.sstG, SSTable, novaGen)
//			//os.removefiles(node.sstg)
//			//os.removefiles(sstable)
//			if err != nil {
//				return err
//			}
//
//=======
//			err, temp := sst.Merge(node.sstG, SSTable, novaGen)
//			if err != nil {
//				fmt.Println(err)
//				return err
//			}
//
//			//removeFiles(int32(node.sstG))
//			//removeFiles(int32(SSTable))
//			fmt.Println(temp)
//>>>>>>> 74927af1c413fc2d6b93c63f2d2bc5f5adc76379
//			if temp > config.MEMTABLE_THRESHOLD*int(math.Pow(2, float64(node.lvl))) {
//				if node.next == nil {
//					node.next = &LSMNode{
//						sstG: -1,
//						next: nil,
//						lvl:  node.lvl + 1,
//					}
//				}
//				node.sstG = -1
//<<<<<<< HEAD
//				return lsm.insertInNode(novaGen, node.next)
//=======
//				lsm.insertInNode(novaGen, node.next)
//>>>>>>> 74927af1c413fc2d6b93c63f2d2bc5f5adc76379
//			} else {
//				node.sstG = novaGen
//			}
//		}
//	}
//
//	return nil
//}
//
//func (lsm *LSMTree) InsertSST(sst int) error {
//	return lsm.insertInNode(sst, lsm.nodes)
//}
//
//func (lsm *LSMTree) FindKey(key []byte) (bool, container.DataNode, error) {
//	var found bool
//	var err error
//	var retVal container.DataNode
//	current := lsm.nodes
//	for current.next != nil {
//		found, retVal, err = Finder.FindKey(key, uint32(current.sstG))
//		if found {
//			return found, retVal, err
//		}
//		current = current.next
//	}
//	found, retVal, err = Finder.FindKey(key, uint32(current.sstG))
//	return found, retVal, err
//}
//
//func (lsm *LSMTree) PrefixScan(key []byte) (bool, []container.DataNode, error) {
//	var found bool
//	var err error
//	var retVal []container.DataNode
//	var tempRetVal []container.DataNode
//	current := lsm.nodes
//	var foundVals map[string]container.DataNode
//	for current != nil {
//		found, tempRetVal, err = Finder.PrefixScan(key, uint32(current.sstG))
//		if found {
//			for _, v := range tempRetVal {
//				_, ok := foundVals[string(v.Key())]
//				if !ok {
//					foundVals[string(v.Key())] = v
//				}
//			}
//		}
//		current = current.next
//	}
//	for _, k := range foundVals {
//		retVal = append(retVal, k)
//	}
//	return found, retVal, err
//}
//
//func (lsm *LSMTree) RangeScan(minKey []byte, maxKey []byte) (bool, []container.DataNode, error) {
//	var found bool
//	var err error
//	var retVal []container.DataNode
//	var tempRetVal []container.DataNode
//	current := lsm.nodes
//	var foundVals map[string]container.DataNode
//	for current != nil {
//		found, tempRetVal, err = Finder.RangeScan(minKey, maxKey, uint32(current.sstG))
//		if found {
//			for _, v := range tempRetVal {
//				_, ok := foundVals[string(v.Key())]
//				if !ok {
//					foundVals[string(v.Key())] = v
//				}
//			}
//		}
//		current = current.next
//	}
//	for _, k := range foundVals {
//		retVal = append(retVal, k)
//	}
//	return found, retVal, err
//}
//
//<<<<<<< HEAD
//func RemoveFiles(generation int32) error {
//
//=======
//func removeFiles(generation int32) error {
//>>>>>>> 74927af1c413fc2d6b93c63f2d2bc5f5adc76379
//	err := os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Data.db")
//	if err != nil {
//		return err
//	}
//	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Index.db")
//	if err != nil {
//		return err
//	}
//	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Summary.db")
//	if err != nil {
//		return err
//	}
//<<<<<<< HEAD
//	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Filter.db")
//	if err != nil {
//		return err
//	}
//=======
//	fmt.Println("EALO")
//	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Filter.db")
//	if err != nil {
//		fmt.Println(err)
//		return err
//	}
//	fmt.Println("LOL")
//>>>>>>> 74927af1c413fc2d6b93c63f2d2bc5f5adc76379
//	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-TOC.txt")
//	if err != nil {
//		return err
//	}
//	err = os.Remove("usertable-" + strconv.Itoa(int(generation)) + "-Metadata.txt")
//	if err != nil {
//		return err
//	}
//	return nil
//<<<<<<< HEAD
//
//}
//
//func (lsmt *LSMTree) Serialize() error {
//	lsmtreeFile, err := os.OpenFile("LSMTree.bin", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
//	if err != nil {
//		return err
//	}
//	var buf bytes.Buffer
//	current := lsmt.nodes
//	for current != nil {
//		err = binary.Write(&buf, binary.BigEndian, current.sstG)
//		err = binary.Write(&buf, binary.BigEndian, current.lvl)
//		current = current.next
//	}
//	_, err = lsmtreeFile.Write(buf.Bytes())
//	return err
//}
//
//func (lsmt *LSMTree) DeserializeLSMT() error {
//	lsmtreeFile, err := os.OpenFile("LSMTree.bin", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
//	//lsmt := LSMTree{max: config.LSM_DEPTH, nodes: nil}
//	if err != nil {
//		return err
//	}
//
//	//lsmt.nodes = &LSMNode{sstG: -1, lvl: 0}
//	current := lsmt.nodes
//
//	var isize int = -1
//
//	lsmtreeFile.Seek(0, 0)
//	mybytes := make([]byte, unsafe.Sizeof(isize))
//	for true {
//
//		_, err = lsmtreeFile.Read(mybytes)
//		if err == io.EOF || err == io.ErrUnexpectedEOF {
//			break
//		}
//		buf := bytes.NewBuffer(mybytes)
//		val, err := binary.ReadVarint(buf)
//		if err == nil {
//			current.sstG = int(val)
//		}
//
//		_, err = lsmtreeFile.Read(mybytes)
//		if err == io.EOF || err == io.ErrUnexpectedEOF {
//			break
//		}
//		buf = bytes.NewBuffer(mybytes)
//		val, err = binary.ReadVarint(buf)
//		if err == nil {
//			current.lvl = int(val)
//		}
//		current.next = &LSMNode{sstG: -1, lvl: current.lvl + 1, next: nil}
//		current = current.next
//	}
//	return err
//}
//
//func (lsmt *LSMTree) GetNextGeneration() int {
//	var gen int = -1
//	current := lsmt.nodes
//	for gen == -1 || current != nil {
//		gen = current.sstG
//		current = current.next
//	}
//	if gen == -1 {
//		return 0
//	} else {
//		return gen + 1
//	}
//}
//=======
//}
//
////type LSMTree struct {
////	rootSS        *memtable.Memtable
////	maxNoOfLevels int // preuzimace se iz config fajla
////	firstLVL      *LSMNode
////	//lastLVL       *LSMNode
////}
////
////type LSMNode struct {
////	next *LSMNode
////	//previous *LSMNode
////	//root *LSMTree
////	sst *SSTable.SSTable
////	//depth      int
////	//noOFLVLPT  *int
////	//hasElement bool
////	leaf bool
////}
////
////func NewLSM(mt memtable.Memtable) LSMTree {
////	return LSMTree{
////		rootSS:        &mt,
////		maxNoOfLevels: config.LSM_DEPTH,
////		firstLVL:      nil,
////		//lastLVL:       nil,
////	}
////}
////
////func (lsmt *LSMTree) InsertSST(sst *SSTable.SSTable) {
////	if lsmt.firstLVL == nil { // ako ne postoji sledeci nivo
////		lsmnod := LSMNode{
////			next: nil,
////			//previous:   nil,
////			//root: lsmt,
////			sst: sst,
////			//hasElement: true,
////			leaf: true,
////		}
////		lsmt.firstLVL = &lsmnod
////		return
////	} else if lsmt.firstLVL.sst == nil { // ako na sledecem nivou nema sstabela
////		lsmt.firstLVL.sst = sst
////	} else { // ako ima tabela na sledecem nivou onda dolazi do mergovanja
////
////	}
////}
////
////func (lsmnod *LSMNode) MergeCaller(sst *SSTable.SSTable) {
////	if lsmnod.sst != nil {
////
////		//ovde ide merdze funkcija da vraca sstable a prima dva sstable
////		// mergeSSTables(lsmnod.sst, sst prosledjeno) SSTable
////		// generacija povratne/mergovane tabele treba da bude veca od generacija
////		// od prosledjenih sstabela +1
////		mergedSST := sst
////		if lsmnod.next == nil {
////			lsmnod.next = &LSMNode{
////				next: nil,
////				//root: lsmt,
////				sst:  mergedSST,
////				leaf: true,
////			}
////		}
////	} else {
////
////	}
////}
//>>>>>>> 74927af1c413fc2d6b93c63f2d2bc5f5adc76379
