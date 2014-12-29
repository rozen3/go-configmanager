package conf

/*
	confmanager keeps the recent data in memory, and persist all records to disk. Read requests will be fast return with the memory data,
	and write will be first applied to disk for safety.
*/

import (
	. "rafted/persist"
	"modules/msgpack"
	//"fmt"
	//"errors"
	"modules/glog"
)

var (
	CM_NOTFOUND_ERR =ErrorConfigNotExist
)

type ConfManager struct {
	dir    string // dir path of data files
	header string // header of data files
	mem    *myList
	disk   *diskIo
}

/******************** public functions ************************/
func GetConfManager(dir, header string) (*ConfManager, error) {
	cm := &ConfManager {
		dir: dir,
		header: header,
		mem: nil,
		disk: nil,
	}

	cm.mem = getMyList()

	disk, err := getDiskIO(dir, header)
	if err != nil {
		return nil, err
	}
	cm.disk = disk

	err = cm.initList()
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func (this *ConfManager) Close() {
	this.mem.close()
	this.disk.close()
}

func (this *ConfManager) PushConfig(logIndex uint64, conf *Config) error {
	buff, err := msgpack.Marshal(conf)
	if err != nil {
		return err
	}

	// push disk
	err = this.disk.append(logIndex, buff)
	if err != nil {
		return err
	}

	// push mem
	listElem := getElem(logIndex, buff)
	err = this.mem.push(listElem)
	if err != nil {
		return nil
	}

	return nil
}

func (this *ConfManager) GetConfig(logIndex uint64) (*ConfigMeta, error) {
	conf := &Config{}

	// get from memory
	memFound := true
	memElem, err := this.mem.get(logIndex)
	if err != nil {
		if err == MEM_NOTFOUND_ERR {
			memFound = false
		} else {
			return nil, err
		}
	}
	if memFound {
		err = msgpack.Unmarshal(memElem.data, conf)
		if err != nil {
			return nil, err
		}

		configMeta := &ConfigMeta {
			FromLogIndex: memElem.startId,
			ToLogIndex: memElem.endId,
			Conf: conf,
		}

		return configMeta, nil
	}

	// if not found in memory, try to read from disk
	startId, endId, buff, err := this.disk.get(logIndex)
	if err != nil {
		if err == DISK_NOTFOUND_ERR {
			return nil, CM_NOTFOUND_ERR
		}
		return nil, err
	}
	err = msgpack.Unmarshal(buff, conf)
	if err != nil {
		return nil, err
	}

	configMeta := &ConfigMeta {
		FromLogIndex: startId,
		ToLogIndex: endId,
		Conf: conf,
	}

	return configMeta, nil
}

func (this *ConfManager) LastConfig() (*ConfigMeta, error) {
	lastElem, err := this.mem.last()
	if err != nil {
		if err == MEM_NOTFOUND_ERR {
			return nil, CM_NOTFOUND_ERR
		}
		return nil, err
	}

	conf := &Config {}
	err = msgpack.Unmarshal(lastElem.data, conf)
	if err != nil {
		return nil, err
	}

	configMeta := &ConfigMeta {
		FromLogIndex: lastElem.startId,
		ToLogIndex: lastElem.endId,
		Conf: conf,
	}

	return configMeta, nil
}

func (this *ConfManager) ListAfter(logIndex uint64) ([]*ConfigMeta, error) {
	result := make([]*ConfigMeta, 0)

	// read mem
	memElems, err := this.mem.listAfter(logIndex)
	if err != nil && err != MEM_NOTFOUND_ERR {
		return nil, err
	}
	memCMs, err := memElemsToConfigMetas(memElems)
	if err != nil {
		return nil, err
	}

	// read others from disk
	if memElems[0].startId > logIndex {
		// need read [logIndex, memElems[0].startId] from disk
		//fmt.Printf("need [%d, %d] from disk\n", logIndex, memElems[0].startId - 1)
		diskElems, err := this.disk.listBetween(logIndex, memElems[0].startId - 1)
		if err != nil {
			if err == DISK_NOTFOUND_ERR {
				return nil, CM_NOTFOUND_ERR
			}
			return nil, err
		}

		diskCMs, err := diskElemsToConfigMetas(diskElems)
		if err != nil {
			return nil, err
		}
		result = append(result, diskCMs...)
		//fmt.Println("diskElemsNum:", len(diskCMs))
	}

	result = append(result, memCMs...)
	//fmt.Println("memElemsNum:", len(memCMs))

	return result, nil
}


func (this *ConfManager) TruncateBefore(logIndex uint64) error {
	// truncate from disk
	err := this.disk.truncateBefore(logIndex)
	if err != nil {
		glog.Errorf("truncateBefore %d faild:%s\n", logIndex, err.Error())
		return err
	}

	// truncate from mem
	err = this.mem.truncateBefore(logIndex)
	if err != nil {
		return err
	}

	return nil
}

func (this *ConfManager) TruncateAfter(logIndex uint64) error {
	// truncate from disk
	err := this.disk.truncateAfter(logIndex)
	if err != nil {
		return err
	}

	// truncate from mem
	_, err = this.mem.truncateAfter(logIndex)
	if err != nil {
		return err
	}

	return nil
}

/**************** internal functions ***********************************/

func memElemsToConfigMetas(memElems []*myElem) ([]*ConfigMeta, error) {
	count := len(memElems)
	result := make([]*ConfigMeta, count)

	for i, e := range memElems {
		meta, err := memElemToConfigMeta(e)
		//result[i], err := memElemToConfigMeta(e)
		result[i] = meta
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func memElemToConfigMeta(memElem *myElem) (*ConfigMeta, error) {
	cm := &ConfigMeta{
		FromLogIndex: memElem.startId,
		ToLogIndex: memElem.endId,
		Conf: &Config{},
	}

	err := msgpack.Unmarshal(memElem.data, cm.Conf)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func diskElemsToConfigMetas(diskElems []*diskElem) ([]*ConfigMeta, error) {
	count := len(diskElems)
	result := make([]*ConfigMeta, count)

	for i, e := range diskElems {
		meta, err := diskElemToConfigMeta(e)
		//result[i], err := memElemToConfigMeta(e)
		result[i] = meta
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func diskElemToConfigMeta(diskElem *diskElem) (*ConfigMeta, error) {
	cm := &ConfigMeta{
		FromLogIndex: diskElem.startId,
		ToLogIndex: diskElem.endId,
		Conf: &Config{},
	}

	err := msgpack.Unmarshal(diskElem.buff, cm.Conf)
	if err != nil {
		return nil, err
	}

	return cm, nil
}

func diskElemToConfig(diskElem *diskElem) (*Config, error) {
	conf := &Config{}
	err := msgpack.Unmarshal(diskElem.buff, conf)
	if err != nil {
		return nil, err
	}

	return conf, nil
}

/*
	read data of the latest file from disk and push them into list
 */
func (this *ConfManager) initList() error {
	elems, err := this.disk.listOfLatestFile()
	if err != nil {
		return err
	}

	for _, e := range elems {
		listElem := getElem(e.startId, e.buff)
		err = this.mem.push(listElem)
		if err != nil {
			return err
		}
	}

	return nil
}

/*
// ConfigManager is the interface for durable config management.
// It provides functions to store and restrieve config.
type ConfigManager interface {
	// Store a new config at the log entry with specified index
	PushConfig(logIndex uint64, conf *Config) error

	// Return the config for specified log index
	GetConfig(logIndex uint64) (*ConfigMeta, error)

	// Return the last config
	LastConfig() (*ConfigMeta, error)

	// Delete the config metadata before and up to the given index,
	// including the given index.
	TruncateBefore(logIndex uint64) error

	// Delete the config metadata after the given index,
	// including the given index. And returns the config
	// at the log entry right before the given index.
	TruncateAfter(logIndex uint64) error
}
*/

