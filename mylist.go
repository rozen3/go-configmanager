package conf

/*
mylist is a skiplist. each elem is made up of startId, endId and data.

[level 3] head=========================>(98,90)==============================================>tail [level 3]
           ||                              ||                                                  ||
[level 2] head=========================>(98,90)==============================>(5,3)==========>tail [level 2]
           ||                              ||                                 ||              ||
[level 1] head==>(max,110)=============>(98,90)============>(15,12)===========>(5,3)==========>tail [level 1]
           ||      ||                     ||                  ||               ||              ||
[level 0] head==>(max,110)==>(109,99)==>(98,90)==>(89,16)==>(15,12)==>(11,6)==>(5,3)==>(2,1)==>tail [level 0]

For example, to find 13 from the list above, we have to follow this track:
head[level 3]->head[level 2]->(98,90)[level 2]->(98,90)[level 1]->(15,12)[level 1]

In our scene, it's more appropriate that elements in list sorted from large to small.(search, insert or delete often by the biggest id)
 */



import (
	"math/rand"
	"time"
	"sync"
	"errors"
	"fmt"
	"math"
	"modules/glog"

)

var (
	MAX_RECORD_NUM int  = 1000 // how many elements it keeps in memory at most
	MAX_LEVEL_LIMIT int = 10   // recommended best set this value to log(MAX_RECORD_NUM)
	// when the level(randomly get) of an element is larger than MAX_LEVEL_LIMIT, will be set to MAX_LEVEL_LIMIT
	NUM_PER_TRUNCATE int = 100 // truncate some old data when reach the MAX_RECORD_NUM
	UINT64_MAX uint64    = math.MaxUint64
	MAX_RESULT_NUM       = 10000 // when call List(), you can at most MAX_RESULT_NUM records per time
)

var (
	MEM_NOTFOUND_ERR = errors.New("mylist.go:NOT FOUND IN MYLIST")
)

type myList struct {
	rnd *rand.Rand
	sum      int // how many elements in all (including both n levels)
	maxLevel int // max level of the list in fact
	head *myNode
	tail *myNode
	lock *sync.Mutex
}

/*
As the list described before, head.levels[3].next points to myNode(90,98)。 In fact, the next node is myNode(90,98).level[3]，
we travel to the next element with the same level specified.
 */

type myElem struct {
	startId uint64
	endId   uint64
	data    []byte
}

type levelPointer struct {
	next *myNode
}

type myNode struct {
	*myElem // actual data

	maxLevel int             // max level of this node
	levels   []*levelPointer // points in each level
}

func getMyList() *myList {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	//new header
	head := &myNode{
		myElem: &myElem{},
		maxLevel: MAX_LEVEL_LIMIT,
		levels: make([]*levelPointer, MAX_LEVEL_LIMIT),
	}

	//new tail
	tail := &myNode{
		myElem: &myElem{},
		maxLevel: MAX_LEVEL_LIMIT,
		levels: make([]*levelPointer, MAX_LEVEL_LIMIT),
	}

	for i := 0; i < MAX_LEVEL_LIMIT; i++ {
		head.levels[i] = &levelPointer{}
		head.levels[i].next = tail
	}

	mcl := &myList{
		rnd: rnd,
		sum: 0,
		maxLevel:0,
		head: head,
		tail: tail,
		lock: new(sync.Mutex),
	}

	return mcl
}

func (this *myList) close() {
	this.head = nil
}

// push an elem to list
func (this *myList) push(e *myElem) error {
	// check if reach the max limit
	if this.sum >= MAX_RECORD_NUM {
		// delete a few records
		this.truncateSome(NUM_PER_TRUNCATE)
	}

	maxLevel := this.getLevel()
	newNode := getNode(e, maxLevel)

	if ok := this.isLatest(newNode); !ok {
		return errors.New(fmt.Sprintf("new node appended is not the latest:%V\n", newNode))
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	if maxLevel > this.maxLevel {
		this.maxLevel = maxLevel
	}

	//for each level
	for nowLevel := newNode.maxLevel; nowLevel >= 0; nowLevel-- {
		//fmt.Println("push in level ", nowLevel)

		newNode.levels[nowLevel].next = this.head.levels[nowLevel].next
		this.head.levels[nowLevel].next = newNode
	}

	//assign endId of the next element
	newNode.levels[0].next.endId = newNode.startId - 1

	//save latest to header
	this.head.myElem = e

	//if this is the first element
	this.sum++
	if this.sum == 1 {
		this.tail.myElem = e
	}

	return nil
}

func (this *myList) last() (*myElem, error) {
	if this.sum <= 0 {
		return nil, MEM_NOTFOUND_ERR
	}

	return this.head.myElem, nil
}

/*
 e.g.
 list: (?, 16)(15, 11)(10,7)(6,3)(2,1)
 truncate after id 8
 new list: (?, 7)(6,3)(2,1)

 e.g.
 list: (?, 16)(10,7)(2,1)  # not level 0
 truncate after id 5
 new list: (?, 16)(10,7)
 */
func (this *myList) truncateAfter(logIndex uint64) (*myElem, error) {
	this.lock.Lock()
	defer this.lock.Unlock()

	//var resultConf *persist.Config = nil
	var resultData *myElem = nil

	//for each levels
	for nowLevel := this.maxLevel; nowLevel >= 0; nowLevel-- {
		// for each level, find the truncate position of node x : whether x.startId <= logIndex <= x.endId  (x contains logIndex)
		// or x.startId > logIndex > x.next.endId (x > logIndex > x.next)
		// finally, head->x->...->tail
		for tmpNode := this.head.levels[nowLevel].next; ; tmpNode = tmpNode.levels[nowLevel].next {
			if tmpNode == this.tail { // truncate all
				this.head.levels[nowLevel].next = this.tail
				break
			}

			cmp := tmpNode.compareTo(logIndex)
			if cmp < 0 { // stop
				this.head.levels[nowLevel].next = tmpNode
				break
			} else if cmp == 0 { // cut
				this.head.levels[nowLevel].next = tmpNode

				if resultData == nil {
					resultData = tmpNode.myElem
				}

				break
			} else { // go forward
				if nowLevel == 0 {
					this.sum--
				}
			}
		}
	} // end of each level

	this.head.myElem = resultData
	this.head.levels[0].next.endId = UINT64_MAX

	if resultData == nil {
		if this.sum <= 0 {
			return nil, MEM_NOTFOUND_ERR
		}
		return nil, errors.New("i dont know")
	}

	return resultData, nil
}

func (this *myList) get(logIndex uint64) (*myElem, error) {
	// start from level max
	// case x in tmp.next: hits
	// case tmp.next > x: go head
	// case tmp.next < x: drop a level (if the level is already 0, return err)
	nowLevel := this.maxLevel
	tmpNode := this.head
	for {
		//if reach the tail, try to find data from disk
		//if tmpNode.levels[nowLevel].next == this.tail {
		if tmpNode == this.tail {
			return nil, MEM_NOTFOUND_ERR
		}

		cmp := tmpNode.levels[nowLevel].next.compareTo(logIndex)
		//fmt.Printf("next elem:(%d,%d)<level %d>, x:%d, cmp:%d\n",
			//tmpNode.levels[nowLevel].next.startId, tmpNode.levels[nowLevel].next.endId, nowLevel, logIndex, cmp)
		if cmp == 0 { // hit
			return tmpNode.levels[nowLevel].next.myElem, nil
		} else if cmp > 0 { // go ahead
			tmpNode = tmpNode.levels[nowLevel].next
		} else {
			if nowLevel == 0 {
				// this scene may never happen because there are no holes in the list.
				return nil, errors.New(fmt.Sprintf("cound not find data for specified logindex: %d", logIndex))
			}

			nowLevel--
		}

		//fmt.Printf("tmp change to (%d,%d)<level %d>, \n", tmpNode.startId, tmpNode.endId, nowLevel)
		if tmpNode == nil {
			// impossible to get here, but leave an exit just in case it happens
			return nil, errors.New("what happend?")
		}
	}

	return nil, errors.New(fmt.Sprintf("cound not find data for specified logindex: %d", logIndex))
}

func (this *myList) listAfter(logIndex uint64) ([]*myElem, error) {
	count := 0 // how many elems to return

	// find the pos
	for tmpNode := this.head.levels[0].next; tmpNode != this.tail; tmpNode = tmpNode.levels[0].next {
		cmp := tmpNode.compareTo(logIndex)
		if cmp < 0 {
			break
		}

		count++
	}
	if count > MAX_RESULT_NUM {
		count = MAX_RESULT_NUM
	}

	resultElems := make([]*myElem, count)
	for tmpNode := this.head.levels[0].next; count > 0; tmpNode = tmpNode.levels[0].next {
		count--
		resultElems[count] = tmpNode.myElem
	}

	return resultElems, nil
}

func (this *myList) list() ([]*myElem, error) {
	resultElems := make([]*myElem, this.sum)

	// get from memory
	i := this.sum - 1
	for tmpNode := this.head.levels[0].next; tmpNode != this.tail; tmpNode = tmpNode.levels[0].next {
		resultElems[i] = tmpNode.myElem
		i--
	}

	return resultElems, nil
}

func (this *myList) truncateBefore(logIndex uint64) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	//delete from memory
	//for each levels
	count := 0
	for nowLevel := this.maxLevel; nowLevel >= 0; nowLevel-- {
		for tmpNode := this.head; tmpNode != this.tail; tmpNode = tmpNode.levels[nowLevel].next {
			nextNode := tmpNode.levels[nowLevel].next
			cmp := nextNode.compareTo(logIndex)
			if cmp < 0 { // if tmp > x > next, truncate from next to tail(result: ...tmp->tail)
				tmpNode.levels[nowLevel].next = this.tail
				break
			} else if cmp == 0 { // if x is in next[start, end], cut next to [x, end] (result: tmp->next[x, end]->tail)
				nextNode.levels[nowLevel].next = this.tail

				if nowLevel == 0 {
					count++
					nextNode.startId = logIndex
					this.tail.myElem = nextNode.myElem
				}

				break
			} else { // if x < next,go forward
				if nowLevel == 0 {
					count++
				}
			}
		}
	} // end of each level

	//update sum
	this.sum = count

	return nil
}

func (this *myList) truncateSome(n int) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	if n >= this.sum {
		for nowLevel := this.maxLevel; nowLevel >= 0; nowLevel-- {
			this.head.levels[nowLevel].next = this.tail
		}
		this.sum = 0

		return nil
	}

	targetSum := this.sum - n
	//fmt.Println("sum, n, target:", this.sum, n, targetSum)

	// find the position to delete from, this node will be the tail in finally
	var positionNode *myNode = nil
	count := 0
	for tmpNode := this.head.levels[0].next; tmpNode != this.tail; tmpNode = tmpNode.levels[0].next {
		count++
		if count >= targetSum {
			positionNode = tmpNode
			break
		}
	}

	// didn't found the position
	if positionNode == nil {
		glog.Errorf("truncateSome error, something might wrong, elems count less than the sum\n")
		return errors.New("truncateSome error, something might wrong, elems count less than the sum\n")
	}

	// delete from level 0
	positionNode.levels[0].next = this.tail

	// delete from other levels
	minId := positionNode.startId
	for nowLevel := this.maxLevel; nowLevel >= 1; nowLevel-- {
		for tmpNode := this.head; tmpNode != this.tail; tmpNode = tmpNode.levels[nowLevel].next {
			if tmpNode.levels[nowLevel].next.startId < minId {
				tmpNode.levels[nowLevel].next = this.tail
				break
			}
		}
	}

	this.sum = targetSum

	return nil
}

// return true if the node to push is latest
func (this *myList) isLatest(n *myNode) bool {
	if n.startId > this.head.startId {
		return true
	} else if this.sum == 0 {
		return true
	}

	return false
}

//while insert an element, get a level randomly, return value between [0, MAX_LEVEL_LIMIT-1]
func (this *myList) getLevel() int {
	level := 0
	for i := 0; i < MAX_LEVEL_LIMIT; i++ {
		level = i

		// random between [0, 1]，50% possibility to level++
		if this.rnd.Intn(100) < 50 {
			break;
		}
	}

	return level
}

func (this *myList) print() {
	fmt.Println("===Print myList:===")
	fmt.Printf("sum:%d\n", this.sum)
	fmt.Printf("maxlevel:%d\n", this.maxLevel)
	fmt.Printf("MAX_LEVEL_LIMIT:%d\n", MAX_LEVEL_LIMIT)
	fmt.Printf("MAX_RECORD_NUM:%d\n", MAX_RECORD_NUM)
	fmt.Printf("MAX_RECORD_NUM:%d\n", NUM_PER_TRUNCATE)

	this.head.print()
	for tmpNode := this.head.levels[0].next; tmpNode != this.tail; tmpNode = tmpNode.levels[0].next {
		tmpNode.print()
	}
	this.tail.print()

	for i := this.maxLevel; i >= 0; i-- {
		fmt.Printf("<level %d> ", i)
		for tmp := this.head.levels[i].next; tmp != this.tail; tmp = tmp.levels[i].next {
			fmt.Printf("(%d,%d)", tmp.startId, tmp.endId)
		}
		fmt.Println()
	}

	fmt.Println("===Print myList end===")
}

func (n *myNode) print() {
	fmt.Printf("<myNode>")
	fmt.Printf("startId:%d ", n.startId)
	fmt.Printf("endId:%d ", n.endId)
	fmt.Printf("maxLevel:%d ", n.maxLevel)
	fmt.Println("</myNode>")
}

/*
	compare a node to an id, return compare result
	return 0: id is in the range of this node
	return -1: node is less than id
	return 1: node is bigger the id
 */
func (this *myNode) compareTo(id uint64) int {
	if this.endId < id {
		return -1
	} else if this.startId > id {
		return 1
	} else {
		return 0
	}
}

// create an elem
func getElem(logIndex uint64, data []byte) *myElem {
	dataLen := len(data)
	e := &myElem {
		startId: logIndex,
		endId: UINT64_MAX,
		data: make([]byte, dataLen),
	}
	copy(e.data, data)

	return e
}

// create a node
func getNode(elem *myElem, maxLevel int) *myNode {
	newNode := &myNode{
		myElem: elem,
		maxLevel: maxLevel,
		levels: make([]*levelPointer, maxLevel+1),
	}

	for i := 0; i <= newNode.maxLevel; i++ {
		newNode.levels[i] = &levelPointer{}
	}

	return newNode
}
