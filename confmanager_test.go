package conf

import (
	"testing"
	. "rafted/persist"
	"modules/msgpack"
	//"fmt"
)

var (
	ISP_CTL = "ctl"
	ISP_CNC = "cnc"
	ISP_EDU = "edu"
	PROTOCOL = "test"
	IP = "192.168.0.1"
	PORT = uint16(10000)

	START_ID = 1000
	ID_RANGE = 100
	DATAFILE_PATH = "./data"
	DATAFILE_HEADER = "conf_mgr"
)

//func getConf(id uint64) *Config {
//	conf := &Config{}
//
//	addr1 := &ServerAddr{
//		Network: fmt.Sprintf("old Network of id: %d", id),
//		IP: fmt.Sprintf("old IP of id: %d", id),
//		Port: uint16(id),
//	}
//	addr2 := &ServerAddr{
//		Network: fmt.Sprintf("new Network of id: %d", id),
//		IP: fmt.Sprintf("new IP of id: %d", id),
//		Port: uint16(id),
//	}
//
//	conf.Servers = append(conf.Servers, *addr1)
//	conf.NewServers = append(conf.NewServers, *addr2)
//
//	return conf
//}

func pushConf(cm *ConfManager, startId int, idRange int, count int) error {
	for i := 0; i < count; i++ {
		id := startId + (i * idRange)
		conf := getConf(id)
		if err := cm.PushConfig(uint64(id), conf); err != nil {
			return err
		}
	}

	return nil
}

func Test_GetConfManager(t *testing.T) {
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()
}

func Test_PushConfig(t *testing.T) {
	removeAll(DATAFILE_PATH)
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()

	count := 100
	err = pushConf(cm, START_ID, ID_RANGE, count)
	if err != nil {
		t.Error(err)
		return
	}

	elems, err := cm.mem.list()
	if err != nil {
		t.Error(err)
		return
	}

	i := 1
	for _, e := range elems {
		startId := START_ID + ID_RANGE * (i - 1)
		endId :=  START_ID + ID_RANGE * (i) - 1

		//unmarshall
		r := &Config{}
		err = msgpack.Unmarshal(e.data, &r)
		if err != nil {
			//fmt.Println("Unmarshal", err)
			t.Error(err)
			return
		}

		//fmt.Println(e.startId, e.endId, r)
		if e.startId != uint64(startId) {
			t.Fatalf("Test_PushConfig failed, get elem[%d, %d], but expected is [%d, %d]\n", e.startId, e.endId, startId, endId)
		}

		i++
	}
}

func Test_LastConfig(t *testing.T) {
	removeAll(DATAFILE_PATH)
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()

	confmeta, err := cm.LastConfig()
	if err != nil {
		if err != CM_NOTFOUND_ERR {
			t.Fatal(err)
			return
		}
	}

	count := 534
	err = pushConf(cm, START_ID, ID_RANGE, count)
	if err != nil {
		t.Error(err)
		return
	}

	confmeta, err = cm.LastConfig()
	if err != nil {
		t.Error(err)
		return
	}

	conf1 := getConf(START_ID + ID_RANGE * (count - 1))

	if !MultiAddrSliceEqual(conf1.Servers, confmeta.Conf.Servers) {
		t.Fatalf("LastConfig elem not correct, expected %#v but get %#v\n", conf1.Servers.Addresses[0], conf1.NewServers.Addresses[0].Addresses[0])
		return
	}
}


//GetConfig
func Test_GetConfig(t *testing.T) {
	removeAll(DATAFILE_PATH)
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()

	count := 1000
	err = pushConf(cm, START_ID, ID_RANGE, count)
	if err != nil {
		t.Error(err)
		return
	}

	testIdx := 1954
	conf, err := cm.GetConfig(uint64(testIdx))
	if err != nil {
		t.Error(err)
		return
	}

	conf1 := getConf(testIdx - (testIdx % ID_RANGE))

	if !MultiAddrSliceEqual(conf.Conf.Servers, conf1.Servers) {
		t.Fatalf("GetConfig elem not correct, expected %#v, but get %#v\n", conf.Conf.Servers.String(), conf1.Servers.String())
		return
	}
}

//ListAfter
func Test_ListAfter(t *testing.T) {
	removeAll(DATAFILE_PATH)
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()

	count := 1000
	err = pushConf(cm, START_ID, ID_RANGE, count)
	if err != nil {
		t.Error(err)
		return
	}

	testIdx := 13579
	metas, err := cm.ListAfter(uint64(testIdx))
	if err != nil {
		t.Error(err)
		return
	}
	//fmt.Println(len(metas))

	start := uint64(testIdx - (testIdx % ID_RANGE))
	i := uint64(0)
	for _, m := range metas {
		startId := start + i * 100
		endId := startId + 99
		if m.FromLogIndex != startId {
			t.Errorf("ListAfter error, expected[%d, %d] but get [%d, %d]\n", startId, endId, m.FromLogIndex, m.ToLogIndex)
			return
		}
		i++
	}

	// try to delete some elems in the range of [0, 57800] from memory ,pretend that memory space is not enough to keep them
	// then call for elems in the range of [42164, ...]
	// confmanager will read them from disk when find some elems are not exist in the memory, so we'll still get the right results
	cm.mem.truncateBefore(57800)
	testIdxNew := 42164
	metas1, err := cm.ListAfter(uint64(testIdxNew))
	if err != nil {
		t.Error(err)
		return
	}

	start1 := testIdxNew - (testIdxNew % ID_RANGE)
	i1 := 0
	for _, m := range metas1 {
		startId1 := start1 + i1 * ID_RANGE
		endId1 := startId1 + ID_RANGE - 1
		//fmt.Printf("%+v\n", m)
		if m.FromLogIndex != uint64(startId1) {
			t.Errorf("ListAfter error, expected[%d, %d] but get [%d, %d]\n", startId1, endId1, m.FromLogIndex, m.ToLogIndex)
			return
		}
		i1++
	}
}

//TruncateBefore
func Test_TruncateBefore(t *testing.T) {
	removeAll(DATAFILE_PATH)
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()

	count := 1000
	// the biggest idx is START_ID + ID_RANGE * (count - 1) = 100900
	// push elems in range [1000, 100900]
	err = pushConf(cm, START_ID, ID_RANGE, count)
	if err != nil {
		t.Error(err)
		return
	}

	// truncate before 35707, so remain elems are in range [35800, 100900]
	testIdx := 35707
	err = cm.TruncateBefore(uint64(testIdx))
	if err != nil {
		t.Error(err)
		return
	}

	// get an elem of smaller index, it must have been truncated
	smallerIdx := 27384
	_, err = cm.GetConfig(uint64(smallerIdx))
	if err != nil {
		if err != CM_NOTFOUND_ERR {
			t.Error(err)
			return
		}
	} else {
		t.Error("Test_TruncateBefore cm faild: why can i get an inexistent elem?")
		return
	}

	// get an elem with idx in  [35800, 100900], it must be there
	biggerIx := 43756
	_, err = cm.GetConfig(uint64(biggerIx))
	if err != nil {
		t.Error(err)
		return
	}

	// truncate all
	veryBigIdx := 100900 + 123 // an idx bigger than the biggest idx
	err = cm.TruncateBefore(uint64(veryBigIdx))
	if err != nil {
		t.Error(err)
		return
	}

	// then you can't get anything
	anyIdx := 53290
	_, err = cm.GetConfig(uint64(anyIdx))
	if err != nil {
		if err != CM_NOTFOUND_ERR {
			t.Error(err)
			return
		}
	} else {
		t.Error("Test_TruncateBefore cm faild: why can i get an inexistent elem?")
		return
	}

//	// check mem
//	_, err = cm.mem.get(27384)
//	if err != nil {
//		if err != MEM_NOTFOUND_ERR {
//			t.Error(err)
//			return
//		}
//	} else {
//		t.Error("Test_TruncateBefore mem faild: why can i get an inexistent elem?")
//		return
//	}
//
//	// check disk
//	_, err = cm.disk.get(27384)
//	if err != nil {
//		if err != DISK_NOTFOUND_ERR {
//			t.Error(err)
//			return
//		}
//	} else {
//		t.Error("Test_TruncateBefore disk faild: why can i get an inexistent elem?")
//		return
//	}
}

func Test_TruncateAfter(t *testing.T) {
	removeAll(DATAFILE_PATH)
	cm, err := GetConfManager(DATAFILE_PATH, DATAFILE_HEADER)
	if err != nil {
		t.Error(err)
		return
	}
	defer cm.Close()

	// push elems in range [1000, 100900]
	count := 1000
	err = pushConf(cm, START_ID, ID_RANGE, count)
	if err != nil {
		t.Error(err)
		return
	}

	// now remain is [1000, 35747]
	testIdx := 35747
	err = cm.TruncateAfter(uint64(testIdx))
	if err != nil {
		t.Error(err)
		return
	}

	// check last elem
	conf1 := getConf(testIdx - (testIdx % ID_RANGE))
	lastConf, err := cm.LastConfig()
	if err != nil {
		t.Error(err)
		return
	}

	if !MultiAddrSliceEqual(conf1.Servers, lastConf.Conf.Servers) {
		t.Fatalf("Test_TruncateAfter error, expected:%s, but get %s\n", conf1.Servers.String(), lastConf.Conf.Servers)
	}

	// try to get an elem in [1000, 35747]
	someIdx := 21968
	confmeta, err := cm.GetConfig(uint64(someIdx))
	if err != nil {
		t.Error(err)
		return
	} else {
		conf2 := getConf(someIdx - (someIdx % ID_RANGE))
		if !MultiAddrSliceEqual(conf2.Servers, confmeta.Conf.Servers) {
			t.Fatalf("Test_TruncateAfter error, expected:%s, but get %s\n", conf2.Servers.String(), confmeta.Conf.Servers)
		}

		return
	}

	// try to get an elem with bigger idx, in our design, the last elem will be returned
	biggerIdx := 35757
	confmeta1, err := cm.GetConfig(uint64(biggerIdx))
	if err != nil {
		t.Error(err)
		return
	} else {
		lastConf, err := cm.LastConfig()
		if err != nil {
			t.Error(err)
			return
		}

		if !MultiAddrSliceEqual(lastConf.Conf.Servers, confmeta1.Conf.Servers) {
			t.Fatalf("Test_TruncateAfter error, expected:%s, but get %s\n", lastConf.Conf.Servers.String(), confmeta1.Conf.Servers)
		}

		return
	}
}

// generate test data
func getConf(ID int) *Config {
	id := uint16(ID)

	// old
	oldAddrSlice := &ServerAddressSlice {
		Addresses: make([]*ServerAddress, 2),
	}
	oldAddrs1 := &ServerAddress{
		Addresses: make([]*Address, 3),
	}
	oldAddrs2 := &ServerAddress{
		Addresses: make([]*Address, 3),
	}
	oldAddrSlice.Addresses[0] = oldAddrs1
	oldAddrSlice.Addresses[1] = oldAddrs2

	oldAddrs1.Addresses[0] = &Address {
		Isp: ISP_CTL,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(0),
	}

	oldAddrs1.Addresses[1] = &Address {
		Isp: ISP_CNC,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(1),
	}

	oldAddrs1.Addresses[2] = &Address {
		Isp: ISP_EDU,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(2),
	}

	oldAddrs2.Addresses[0] = &Address {
		Isp: ISP_CNC,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + uint16(10),
	}

	oldAddrs2.Addresses[1] = &Address {
		Isp: ISP_CNC,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(11),
	}

	oldAddrs2.Addresses[2] = &Address {
		Isp: ISP_EDU,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(12),
	}

	//new
	newAddrSlice := &ServerAddressSlice {
		Addresses: make([]*ServerAddress, 2),
	}
	newAddrs1 := &ServerAddress{
		Addresses: make([]*Address, 3),
	}
	newAddrs2 := &ServerAddress{
		Addresses: make([]*Address, 3),
	}
	newAddrSlice.Addresses[0] = newAddrs1
	newAddrSlice.Addresses[1] = newAddrs2

	newAddrs1.Addresses[0] = &Address {
		Isp: ISP_CTL,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(0),
	}

	newAddrs1.Addresses[1] = &Address {
		Isp: ISP_CNC,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(1),
	}

	newAddrs1.Addresses[2] = &Address {
		Isp: ISP_EDU,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(2),
	}

	newAddrs2.Addresses[0] = &Address {
		Isp: ISP_CNC,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(10),
	}

	newAddrs2.Addresses[1] = &Address {
		Isp: ISP_CNC,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(11),
	}

	newAddrs2.Addresses[2] = &Address {
		Isp: ISP_EDU,
		Protocol: PROTOCOL,
		IP: IP,
		Port: PORT + id + uint16(12),
	}

	// config
	conf := &Config{
		Servers: oldAddrSlice,
		NewServers: newAddrSlice,
	}

	return conf
}
