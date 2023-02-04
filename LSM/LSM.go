package LSM

import (
	"bytes"
	"encoding/binary"
	"fmt"
	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
	"github.com/c-danil0o/NASP/Finder"
	memtable "github.com/c-danil0o/NASP/Memtable"
	sst "github.com/c-danil0o/NASP/SSTable"
	"io"
	"math"
	"os"
	"strconv"
	"unsafe"
)

type LSMTree struct {
	//memTable memtable.Memtable
	max   int
	nodes *LSMNode
}
type LSMNode struct {
	sstG int
	next *LSMNode
	lvl  int
}

var Active LSMTree

func Init() {
	Active = *NewLSMTree()
	Active.DeserializeLSMT()
	memtable.Generation = uint32(Active.GetNextGeneration())
}

// mem memtable.Memtable
func NewLSMTree() *LSMTree {
	return &LSMTree{
		max: config.LSM_DEPTH,
		nodes: &LSMNode{
			sstG: -1,
			next: nil,
			lvl:  1,
		},
	}
}

func (lsm *LSMTree) insertInNode(SSTable int, node *LSMNode) error {
	if node.sstG == -1 {
		node.sstG = SSTable
	} else {
		if node.lvl == lsm.max {
			fmt.Println("Popunjen je max level LSMa.")
		} else {
			var novaGen int
			if node.sstG > SSTable {
				novaGen = node.sstG + 1
			} else {
				novaGen = SSTable + 1
			}

			err, temp := sst.Merge(node.sstG, SSTable, novaGen)
			//os.removefiles(node.sstg)
			//os.removefiles(sstable)
			if err != nil {
				return err
			}

			if temp > config.MEMTABLE_THRESHOLD*int(math.Pow(2, float64(node.lvl))) {
				if node.next == nil {
					node.next = &LSMNode{
						sstG: -1,
						next: nil,
						lvl:  node.lvl + 1,
					}
				}
				node.sstG = -1
				return lsm.insertInNode(novaGen, node.next)
			} else {
				node.sstG = novaGen
			}
		}
	}

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
	for current.next != nil {
		found, retVal, err = Finder.FindKey(key, uint32(current.sstG))
		if found {
			return found, retVal, err
		}
		current = current.next
	}
	found, retVal, err = Finder.FindKey(key, uint32(current.sstG))
	return found, retVal, err
}

func (lsm *LSMTree) PrefixScan(key []byte) (bool, []container.DataNode, error) {
	var found bool
	var err error
	var retVal []container.DataNode
	var tempRetVal []container.DataNode
	current := lsm.nodes
	var foundVals map[string]container.DataNode
	for current != nil {
		found, tempRetVal, err = Finder.PrefixScan(key, uint32(current.sstG))
		if found {
			for _, v := range tempRetVal {
				_, ok := foundVals[string(v.Key())]
				if !ok {
					foundVals[string(v.Key())] = v
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
	var foundVals map[string]container.DataNode
	for current != nil {
		found, tempRetVal, err = Finder.RangeScan(minKey, maxKey, uint32(current.sstG))
		if found {
			for _, v := range tempRetVal {
				_, ok := foundVals[string(v.Key())]
				if !ok {
					foundVals[string(v.Key())] = v
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
	lsmtreeFile, err := os.OpenFile("LSMTree.bin", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	current := lsmt.nodes
	for current != nil {
		err = binary.Write(&buf, binary.BigEndian, current.sstG)
		err = binary.Write(&buf, binary.BigEndian, current.lvl)
		current = current.next
	}
	_, err = lsmtreeFile.Write(buf.Bytes())
	return err
}

func (lsmt *LSMTree) DeserializeLSMT() error {
	lsmtreeFile, err := os.OpenFile("LSMTree.bin", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	//lsmt := LSMTree{max: config.LSM_DEPTH, nodes: nil}
	if err != nil {
		return err
	}

	//lsmt.nodes = &LSMNode{sstG: -1, lvl: 0}
	current := lsmt.nodes

	var isize int = -1

	lsmtreeFile.Seek(0, 0)
	mybytes := make([]byte, unsafe.Sizeof(isize))
	for true {

		_, err = lsmtreeFile.Read(mybytes)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		buf := bytes.NewBuffer(mybytes)
		val, err := binary.ReadVarint(buf)
		if err == nil {
			current.sstG = int(val)
		}

		_, err = lsmtreeFile.Read(mybytes)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		buf = bytes.NewBuffer(mybytes)
		val, err = binary.ReadVarint(buf)
		if err == nil {
			current.lvl = int(val)
		}
		current.next = &LSMNode{sstG: -1, lvl: current.lvl + 1, next: nil}
		current = current.next
	}
	return err
}

func (lsmt *LSMTree) GetNextGeneration() int {
	var gen int = -1
	current := lsmt.nodes
	for gen == -1 || current != nil {
		gen = current.sstG
		current = current.next
	}
	if gen == -1 {
		return 0
	} else {
		return gen + 1
	}
}

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
