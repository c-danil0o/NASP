package LSM

import (
	config "github.com/c-danil0o/NASP/Config"
	container "github.com/c-danil0o/NASP/DataContainer"
	"github.com/c-danil0o/NASP/Finder"
	memtable "github.com/c-danil0o/NASP/Memtable"
	"github.com/c-danil0o/NASP/SSTable"
)

type LSMTree struct {
	memTable memtable.Memtable
	max      int
	nodes    *LSMNode
}

type LSMNode struct {
	sst  *SSTable.SSTable
	next *LSMNode
	lvl  int
}

var Active LSMTree

func Init() {
	Active = *NewLSMTree()
}

// mem memtable.Memtable
func NewLSMTree() *LSMTree {
	return &LSMTree{
		//memTable: mem,
		max: config.LSM_DEPTH,
		nodes: &LSMNode{
			sst:  nil,
			next: nil,
			lvl:  1,
		},
	}
}

func (lsm *LSMTree) insertInNode(SSTable *SSTable.SSTable, node *LSMNode) {
	if node.sst == nil {
		node.sst = SSTable
	} else {
		if node.lvl == lsm.max {
			//error
		} else {
			if node.next == nil {
				node.next = &LSMNode{
					//sst:  merge(node.sst, SSTable),
					next: nil,
					lvl:  node.lvl + 1,
				}
				node.sst = nil
			} else {
				lsm.insertInNode(SSTable, node.next)
			}
		}
	}
}

func (lsm *LSMTree) InsertSST(sst *SSTable.SSTable) {
	lsm.insertInNode(sst, lsm.nodes)
}

func (lsm *LSMTree) FindKey(key []byte) (bool, container.DataNode, error) {
	var found bool
	var err error
	var retVal container.DataNode
	current := lsm.nodes
	for current.next != nil {
		found, retVal, err = Finder.FindKey(key, current.sst.Generation)
		if found {
			return found, retVal, err
		}
		current = current.next
	}
	found, retVal, err = Finder.FindKey(key, current.sst.Generation)
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
		found, tempRetVal, err = Finder.PrefixScan(key, current.sst.Generation)
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
		found, tempRetVal, err = Finder.RangeScan(minKey, maxKey, current.sst.Generation)
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

//type LSMTree struct {
//	rootSS        *memtable.Memtable
//	maxNoOfLevels int // preuzimace se iz config fajla
//	firstLVL      *LSMNode
//	//lastLVL       *LSMNode
//}
//
//type LSMNode struct {
//	next *LSMNode
//	//previous *LSMNode
//	//root *LSMTree
//	sst *SSTable.SSTable
//	//depth      int
//	//noOFLVLPT  *int
//	//hasElement bool
//	leaf bool
//}
//
//func NewLSM(mt memtable.Memtable) LSMTree {
//	return LSMTree{
//		rootSS:        &mt,
//		maxNoOfLevels: config.LSM_DEPTH,
//		firstLVL:      nil,
//		//lastLVL:       nil,
//	}
//}
//
//func (lsmt *LSMTree) InsertSST(sst *SSTable.SSTable) {
//	if lsmt.firstLVL == nil { // ako ne postoji sledeci nivo
//		lsmnod := LSMNode{
//			next: nil,
//			//previous:   nil,
//			//root: lsmt,
//			sst: sst,
//			//hasElement: true,
//			leaf: true,
//		}
//		lsmt.firstLVL = &lsmnod
//		return
//	} else if lsmt.firstLVL.sst == nil { // ako na sledecem nivou nema sstabela
//		lsmt.firstLVL.sst = sst
//	} else { // ako ima tabela na sledecem nivou onda dolazi do mergovanja
//
//	}
//}
//
//func (lsmnod *LSMNode) MergeCaller(sst *SSTable.SSTable) {
//	if lsmnod.sst != nil {
//
//		//ovde ide merdze funkcija da vraca sstable a prima dva sstable
//		// mergeSSTables(lsmnod.sst, sst prosledjeno) SSTable
//		// generacija povratne/mergovane tabele treba da bude veca od generacija
//		// od prosledjenih sstabela +1
//		mergedSST := sst
//		if lsmnod.next == nil {
//			lsmnod.next = &LSMNode{
//				next: nil,
//				//root: lsmt,
//				sst:  mergedSST,
//				leaf: true,
//			}
//		}
//	} else {
//
//	}
//}
