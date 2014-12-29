package conf

import (
	"fmt"
	"testing"
)

var (
	DISK_DATA_PATH = "./disk_data"
	DISK_DATA_HEADER = "disk"
)

// push elems, start from 100, push count elems, with each one increases by 100
func pushDiskElems(disk *diskIo, count uint64) error {
	startId := uint64(100)
	for i := uint64(0); i < count; i++ {
		id := uint64(startId + (i * 100))
		//buff := fmt.Sprintf("this is a buff for test %d", id)
		buff := getBuff(int(id))
		if err := disk.append(id, []byte(buff)); err != nil {
			return err
		}
	}

	return nil
}

func Test_getDiskIO(t *testing.T) {
	removeAll(DISK_DATA_PATH)
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()
}

func Test_append(t *testing.T) {
	removeAll(DISK_DATA_PATH)
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	err = pushDiskElems(disk,  15000)
	if err != nil {
		t.Error(err)
		return
	}
}

func Test_last(t *testing.T) {
	removeAll(DISK_DATA_PATH)
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	err = pushDiskElems(disk, 15000)
	if err != nil {
		t.Error(err)
		return
	}

	id, buff, err := disk.last()
	if err != nil {
		t.Error(err)
		return
	}

	if id == 1500000 && string(buff) != "this is a buff for test 15000" {
		//correct
	} else {
		t.Errorf("last failed, expected id is 1500000 but got id:%d, buff:%v\n", id, string(buff))
		return
	}
}

func Test_get1(t *testing.T) {
	removeAll(DISK_DATA_PATH)

	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	err = pushDiskElems(disk, 15000)
	if err != nil {
		t.Error(err)
		return
	}

	_, _, buff, err := disk.get(24325)
	if err != nil {
		t.Error(err)
		return
	} else if string(buff) != "this is a buff for test 24300" {
		t.Errorf("need buff with id %d, but get:%v\n", 24300, string(buff))
		return
	}

	_, _, buff, err = disk.get(123456)
	if err != nil {
		t.Error(err)
		return
	} else if string(buff) != "this is a buff for test 123400" {
		t.Errorf("need buff with id %d, but get:%v\n", 123400, string(buff))
		return
	}
}

//test reopen a data file
func Test_get2(t *testing.T) {
	removeAll(DISK_DATA_PATH)

	olddisk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}

	err = pushDiskElems(olddisk, 33210)
	if err != nil {
		t.Error(err)
		return
	}

	// close
	olddisk.close()

	// open a new one
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	_, _, buff, err := disk.get(2614532)
	if err != nil {
		t.Error(err)
		return
	} else if string(buff) != "this is a buff for test 2614500" {
		t.Errorf("need buff with id %d, but get:%v\n", 2614500, string(buff))
		return
	}

	// get not exist elem
	_, _, buff, err = disk.get(58)
	if err != nil {
		if err != DISK_NOTFOUND_ERR {
			t.Error(err)
			return
		}
	} else {
		t.Error("why get a inexistent elem succeed?")
		return
	}

	_, _, buff, err = disk.get(4265005)
	if err != nil {
		if err != DISK_NOTFOUND_ERR {
			t.Error(err)
			return
		}
	} else {
		t.Error("why get a inexistent elem succeed?")
		return
	}
}

//test reopen a data file with index file missing
func Test_get3(t *testing.T) {
	removeAll(DISK_DATA_PATH)

	olddisk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}

	err = pushDiskElems(olddisk, 33210)
	if err != nil {
		t.Error(err)
		return
	}

	// close
	olddisk.close()

	// delete index file
	removeIndexs(DISK_DATA_PATH)

	// open a new one
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	_, _, buff, err := disk.get(2614513)
	if err != nil {
		t.Error(err)
		return
	} else if string(buff) != "this is a buff for test 2614500" {
		t.Errorf("need buff with id %d, but get:%v\n", 2614500, string(buff))
		return
	}

	// get not exist elem
	_, _, buff, err = disk.get(58)
	if err != nil {
		if err != DISK_NOTFOUND_ERR {
			t.Error(err)
			return
		}
	} else {
		t.Error("why get a inexistent elem succeed?")
	}

	_, _, buff, err = disk.get(4126505)
	if err != nil {
		if err != DISK_NOTFOUND_ERR {
			t.Error(err)
			return
		}
	} else {
		t.Error("why get a inexistent elem succeed?")
		return
	}
}

// test listAfter
func Test_listAfter1(t *testing.T) {
	removeAll(DISK_DATA_PATH)

	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	err = pushDiskElems(disk, 15000)
	if err != nil {
		t.Error(err)
		return
	}

	elems, err := disk.listAfter(578613)
	if err != nil {
		t.Error(err)
		return
	}

	//check num
	if len(elems) != 15000-5786+1 {
		t.Errorf("listAfter failed, num of elems must be %d, but get %d\n", 15000-5786+1, len(elems))
		return
	}

	// check rest elements
	for i, e := range elems {
		nowId := uint64(578600 + i * 100)
		endId := uint64(nowId + 99)
		if e.startId != nowId || (e.endId != endId && e.endId != 0)  {
			t.Errorf("Test_listAfter failed,  expected [%d, %d] but get [%d, %d]\n", e.startId, e.endId, nowId, endId)
			return
		}
	}
}

// truncateBefore
func Test_truncateBefore1(t *testing.T) {
	removeAll(DISK_DATA_PATH)
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	// push [100, 1500000]
	err = pushDiskElems(disk, 15000)
	if err != nil {
		t.Error(err)
		return
	}

	err = disk.truncateBefore(758613)
	if err != nil {
		t.Error(err)
		return
	}

	// get an inexistent elem
	_, _, buff, err := disk.get(521062)
	if err != nil && err != DISK_NOTFOUND_ERR {
		t.Error(err)
		return
	}

	// get an exist elem
	_, _, buff, err = disk.get(1123963)
	if err != nil {;
		t.Error(err)
		return
	} else {
		if string(buff) != "this is a buff for test 1123900" {
			t.Errorf("get after Test_truncateBefore1 failed, buff is %v\n", string(buff))
			return
		}
	}

	// delete index file
	removeIndexs(DISK_DATA_PATH)

	disk1, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk1.close()

	// try again, still get the right result though idx file is missing
	_, _, buff, err = disk1.get(1123963)
	if err != nil {
		t.Error(err)
		return
	} else {
		if string(buff) != "this is a buff for test 1123900" {
			t.Errorf("get after Test_truncateBefore1 failed, buff is %v\n", string(buff))
			return
		}
	}
}

func Test_truncateAfter1(t *testing.T) {
	removeAll(DISK_DATA_PATH)
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	err = pushDiskElems(disk, 15000)
	if err != nil {
		t.Error(err)
		return
	}

	startId, _, err := disk.last()
	if err != nil {
		t.Error(err)
		return
	}

	lastId := 100 + (15000 - 1) * 100
	if startId != uint64(lastId) {
		t.Fatalf("the last elem id must be %d, bug in fact is %d\n", lastId, startId)
	}

	err = disk.truncateAfter(758613)
	if err != nil {
		t.Error(err)
		return
	}

	startId, _, err = disk.last()
	if err != nil {
		t.Error(err)
		return
	}

	//fmt.Println(string(elem.buff))
	if startId != 758600 {
		t.Errorf("truncateAfter failed, startId get %d but expected %d\n", startId, 758600)
		return
	}

	// get an inexistent elem
	_, _, buff, err := disk.get(1122106)
	if err != nil && err != DISK_NOTFOUND_ERR {
		t.Error(err)
		return
	}

	// get an exist elem
	_, _, buff, err = disk.get(207693)
	if err != nil {;
		t.Error(err)
		return
	} else {
		if string(buff) != "this is a buff for test 207600" {
			t.Errorf("get after truncateAfter failed, buff is %v\n", string(buff))
			return
		}
	}

	// delete index file
	removeIndexs(DISK_DATA_PATH)

	// reopen
	disk1, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk1.close()

	// try again
	_, _, buff, err = disk1.get(207693)
	//fmt.Println(string(buff))
	if err != nil {;
		t.Error(err)
		return
	} else {
		if string(buff) != "this is a buff for test 207600" {
			t.Errorf("get after Test_truncateBefore1 failed, buff is %v\n", string(buff))
			return
		}
	}
}

func Test_listBetween(t *testing.T) {
	removeAll(DISK_DATA_PATH)
	disk, err := getDiskIO(DISK_DATA_PATH, DISK_DATA_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer disk.close()

	err = pushDiskElems(disk, 15000)
	if err != nil {
		t.Error(err)
		return
	}

	elems, err := disk.listBetween(578496, 1024573)
	if err != nil {
		t.Error(err)
		return
	}

	count := 10245 - 5784 + 1
	if count != len(elems) {
		t.Errorf("count of elems error, expected %d, but get %d\n", count, len(elems))
		return
	}

	start := uint64(578400)
	i := uint64(0)
	for _, e := range elems {
		startId := start + i * 100
		endId := startId + 99
		if e.startId != startId || e.endId != endId {
			t.Errorf("get elem error, expected [%d, %d] but get [%d, %d]", startId, endId, e.startId, e.endId)
			return
		}

		i++
		//fmt.Printf("%d, %d, %+v\n", e.startId, e.endId, string(e.buff))
	}
}

func getBuff(id int) string {
	return fmt.Sprintf("this is a buff for test %d", id)
}
