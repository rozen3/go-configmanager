package conf

import (
	"testing"
	"fmt"
)

func Test_getMyList(t *testing.T) {
	list := getMyList()
	if list == nil {
		t.Error("getMyList failed")
	}
	defer list.close()
}

func Test_pushAndlast(t *testing.T) {
	list := getMyList()
	defer list.close()
	e := getElem(0, []byte("haha i am the first elem"))

	err := list.push(e)
	if err != nil {
		t.Fatalf("push failed %#v\n", err)
	}

	lastElem, err := list.last()
	if err != nil {
		t.Error("last failed")
	}

	if lastElem.startId != e.startId || string(lastElem.data) != string(e.data) {
		t.Error("last incorrect")
	}
}

func Test_truncateAfter(t *testing.T) {
	list := getMyList()
	defer list.close()

	// build a list, from (100, 200) to (9900, 10000)
	for i := 1; i < 100; i++ {
		e := getElem(uint64(i * 100), []byte(fmt.Sprintf("%d", i)))
		list.push(e)
	}

	// truncate after 8792
	e, err := list.truncateAfter(uint64(8792))
	if err != nil {
		t.Error("truncateAfter failed")
	}

	if e.startId != 8700 {
		t.Error("truncateAfter 8792 failed, the last elem's startId is", e.startId)
	}

	// truncate after 4215
	e, err = list.truncateAfter(uint64(4215))
	if err != nil {
		t.Error("truncateAfter failed")
	}

	if e.startId != 4200 {
		t.Error("truncateAfter 4215 failed, the last elem's startId is", e.startId)
	}

	// truncate after 1236
	e, err = list.truncateAfter(uint64(1236))
	if err != nil {
		t.Error("truncateAfter failed")
	}

	if e.startId != 1200 {
		t.Error("truncateAfter 1236 failed, the last elem's startId is", e.startId)
	}

	// truncate after 67 (no elem will left)
	e, err = list.truncateAfter(uint64(67))
	if err != nil {
		if err == MEM_NOTFOUND_ERR {
			// correct
		} else {
			t.Error("last failed while truncate all elems:", err)
		}
	} else {
		t.Error("didn't i just truncated anything?")
	}
}

func Test_get(t *testing.T) {
	list := getMyList()
	defer list.close()

	// build a list, from (100, 200) to (9900, 10000)
	for i := 1; i < 100; i++ {
		e := getElem(uint64(i * 100), []byte(fmt.Sprintf("%d", i)))
		list.push(e)
	}

	e, err := list.get(uint64(5213))
	if err != nil {
		t.Error("get failed err:", err)
	}

	if e.startId != 5200 || e.endId != 5300 - 1 {
		t.Error("get incorrect, startId, endId:", e.startId, e.endId)
	}

	e, err = list.get(uint64(11))
	if err != nil {
		if err != MEM_NOTFOUND_ERR {
			t.Error("get failed:", err)
		}
	}
}

func Test_listAfter(t *testing.T) {
	list := getMyList()
	defer list.close()
	// build a list, from (100, 200) to (9900, 10000)
	for i := 1; i < 100; i++ {
		e := getElem(uint64(i * 100), []byte(fmt.Sprintf("%d", i)))
		list.push(e)
	}

	elems, err := list.listAfter(uint64(5324))
	if err != nil {
		t.Error("listAfter failed:", err)
	}

	if len(elems) != 99 - 53 + 1{
		t.Errorf("listAfter failed, result num %d, expected", len(elems), 99 - 53 + 1)
		return
	}
//	for _, e := range elems {
//		fmt.Println(e.startId)
//	}
}

func Test_truncateBefore(t *testing.T) {
	list := getMyList()
	defer list.close()
	// build a list, from (100, 200) to (9900, 10000)
	for i := 1; i < 100; i++ {
		e := getElem(uint64(i * 100), []byte(fmt.Sprintf("%d", i)))
		list.push(e)
	}

	//truncate before 5012 (result list will be from (5100, 5200) to (9900, 1000))
	err := list.truncateBefore(uint64(5012))
	if err != nil {
		t.Error("truncateBefore failed:", err)
	}

	// if try to get 4431, will be an error of "logindex 4431 is less than the min startId in lis"
	e, err := list.get(uint64(4431))
	if err != nil {
		if err != MEM_NOTFOUND_ERR {
			t.Error("get failed:", err)
		}
	} else {
		t.Error("truncateBefore failed, how could i get an deleted elem:", e.startId, e.endId)
	}
}

func Test_truncateSomeWhenPush(t *testing.T) {
	list := getMyList()
	defer list.close()
	// build a list, from (100, 200) to (9900, 10000)
	for i := 1; i <= 1001; i++ {
		e := getElem(uint64(i * 100), []byte(fmt.Sprintf("%d", i)))
		list.push(e)
	}

	if list.sum != 1001 - 100 {
		t.Error("Test_truncateSomeWhenPush error, expected 901 but in fact", list.sum)
	}
}

func Test_truncateSome(t *testing.T) {
	list := getMyList()
	defer list.close()
	// build a list, from (100, 200) to (9900, 10000)
	for i := 1; i < 100; i++ {
		e := getElem(uint64(i * 100), []byte(fmt.Sprintf("%d", i)))
		list.push(e)
	}

	if list.sum != 99 {
		t.Error("list sum error")
	}

	// truncate 98
	err := list.truncateSome(98)
	if err != nil {
		t.Error("truncateSome error:", err)
	}

	if list.sum != 1 {
		t.Error("truncateSome error, the sum expexted is 1 but in fact is", list.sum)
	}

	// truncate others
	err = list.truncateSome(1)
	if err != nil {
		t.Error("truncateSome error:", err)
	}

	if list.sum != 0 {
		t.Error("truncateSome error, the sum expexted is 0 but in fact is", list.sum)
	}

	// truncate empty list
	err = list.truncateSome(1)
	if err != nil {
		t.Error("truncateSome error:", err)
	}

	if list.sum != 0 {
		t.Error("truncateSome error, the sum expexted is 0 but in fact is", list.sum)
	}
}

func Test_compareTo(t *testing.T) {
	node := &myNode {
		myElem: getElem(uint64(111), []byte("aaa")),
	}
	node.endId = 222

	if node.compareTo(uint64(123)) != 0 {
		t.Error("compareTo error")
	}

	if node.compareTo(uint64(5)) != 1 {
		t.Error("compareTo error")
	}

	if node.compareTo(uint64(576)) != -1 {
		t.Error("compareTo error")
	}
}
