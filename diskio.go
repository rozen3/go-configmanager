package conf

/*
data filename:
	path/header_startId.data // e.g. CONFIG_0000000001.data (if length of startId is less than FILE_NAME_NUMLEN, will add 0 by front)
index filename:
	path/header_startId.idx

file content fmt: [record][record]...EOF
	[record] = start_id(8 byte)end_id(8byte)buff_len(8 byte)buff(buff_len byte) 0 0 0 0 0 (expand to 512 bytes or n * 512 bytes)

index file content fmt: [file_meta][section_index][section_index]...EOF
	[file_meta] = [data_file_size(8 byte)][record_num(8 byte)]
	              [last_record_pos(8 byte)][minId(8 byte)]
	              [maxId(8 byte)][record_num_level(4 byte)]
	              [size_level(8 byte)]
	[section_index] = start_id(8 byte)pos(8 byte) // save the start position of the first record in each section
	ps: size of the index file will be header + record_num * (8 + 8) = 16 (record_num + 1) . Data file size must be less than 1GB,
		so record_num <= 1GB / 512byte (2097152B), then index file size must be less than (32M + header)

Usage:
	// new
	disk, err := getDiskIO(".", "CONF")
	if err != nil {
		return err
	}
	defer disk.Close()

	// append
	buff :=  []byte("this is a buff for test")
	err := disk.append(123, buff); err != nil {


	// last
	lastId, buff, err := disk.last()

	// get
	buff, err := disk.get(100)

	// listAfter
	buff, err := disk.listAfter(100)

	// truncateBefore
	err := disk.truncateBefore(100)

	// truncateAfter
	diskElem, err := disk.truncateBefore(100)
	fmt.Println(diskElem.startId, diskElem.endId, diskElem.buff)

 */

import (
	"os"
	"errors"
	"path/filepath"
	"strings"
	"strconv"
	"fmt"
	"encoding/binary"
	"io"
	"sort"
	"modules/glog"
)

/****************** constants *******************************/
var (
	// FILENAME
	FILE_NAME_NUMLEN = 10

	// COMMON
	ID_LEN  uint64   = 8 // uint64
	POS_LEN  uint64  = 8 // uint64
	SIZE_LEN  uint64 = 8 // uint64
	NUM_LEN  uint64  = 8 // uint64

	// for each record of data file
	DATA_MAX_FILE_SIZE  uint64 = 1024 * 1024 * 2 // open a new data file if it grows larger than this size
	DATA_BLOCK_SIZE  uint64    = 512             // each record stored in disk must be n times of this size. if not enough, add 0 till the end
	DATA_STARTID_POS  uint64   = 0
	DATA_ENDID_POS  uint64     = DATA_STARTID_POS + ID_LEN
	DATA_BUFFLEN_POS  uint64   = DATA_ENDID_POS + ID_LEN
	DATA_BUFF_POS  uint64      = DATA_BUFFLEN_POS + SIZE_LEN
	DATA_HEAD_SIZE  uint64     = DATA_BUFF_POS

	// for index
	IDX_MAX_SECTION_SIZE  uint64        = 1024 * 1024 // 1MB, each section must less than or equal to this size
	IDX_MAX_RECORD_PER_SECTION   uint64 = 1000        // each section must have no more than MAX_RECORD_PER_SECTION recordsfileMeta
	IDX_DATAFILESIZE_POS  uint64        = 0
	IDX_RECORDNUM_POS  uint64           = IDX_DATAFILESIZE_POS + SIZE_LEN
	IDX_LASTRECORDPOS_POS  uint64       = IDX_RECORDNUM_POS + NUM_LEN
	IDX_MINID_POS  uint64               = IDX_LASTRECORDPOS_POS + POS_LEN
	IDX_MAXID_POS  uint64               = IDX_MINID_POS + ID_LEN
	IDX_RECORDLEVEL_POS  uint64         = IDX_MAXID_POS + ID_LEN
	IDX_SIZELEVEL_POS  uint64           = IDX_RECORDLEVEL_POS + NUM_LEN
	IDX_HEADER_SIZE  uint64             = IDX_SIZELEVEL_POS + SIZE_LEN

	// for each section_index of index file
	SI_STARTID_POS  uint64 = 0
	SI_POS_POS  uint64     = 0 + POS_LEN
	SI_SIZE  uint64        = ID_LEN + POS_LEN
)

var (
	DISK_NOTFOUND_ERR = errors.New("diskio.go:NOT FOUND IN DISK")
)

/***********************  struct define **********************/

type diskIo struct {
	path           string
	header         string
	latestFileName string // last file
	latestFilePtr *os.File
	idxMgr *indexMgr
}

type indexMgr struct {
	mapIndex map[string]*indexInfo // filename ==> indexInfo
}

type indexInfo struct {
	filePtr    *os.File
	meta       fileMeta
	indexs     []*indexElem
	waterLevel waterLevelInfo
}

type fileMeta struct {
	dataFileSize  uint64
	recordNum     uint64
	lastRecordPos uint64
	minId         uint64
	maxId         uint64
}

type indexElem struct {
	startId uint64
	pos     uint64
}

type diskElem struct {
	startId uint64
	endId   uint64
	buff    []byte
}

// if the waterLevel exceeds the threshold, will be cut down by threshold along with add a new index
type waterLevelInfo struct {
	recordCount uint64
	sizeCount   uint64
}

// sort files by startId
type idxMgrSorter struct {
	disk *diskIo
	items []item
}
type item struct {
	fileName string
	indexInfo *indexInfo
}

/*************** public funtions for use *******************/

func getDiskIO(path, header string) (*diskIo, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	//fmt.Println("abspath is:", absPath)

	disk := &diskIo{
		path: absPath,
		header: header,
		latestFileName: "",
		latestFilePtr: nil,
		idxMgr: &indexMgr {
			mapIndex: make(map[string]*indexInfo),
		},
	}

	err = disk.init()
	if err != nil {
		return nil, err
	}

	return disk, nil
}

//close. write index to disk
func (this *diskIo) close() {
	// close index files
	for _, indexInfo := range this.idxMgr.mapIndex {
		//fmt.Println(indexInfo.meta)
		//fmt.Println(indexInfo.waterLevel)

		// write meta
		indexInfo.writeMetaToDisk()

		indexInfo.filePtr.Close()
	}

	// close file
	if this.latestFilePtr != nil {
		this.latestFilePtr.Close()
	}
}

// append an element to file
func (this *diskIo) append(logIndex uint64, buff []byte) error {
	// compared with last startId
	err := this.checkIdValid(logIndex)
	if err != nil {
		return err
	}

	// update the endId of last elem
	if err := this.updateLastElem(logIndex); err != nil {
		return err
	}

	//find the latest file to append
	dataLen := len(buff)
	if err := this.getLatestFileToWrite(logIndex, uint64(dataLen)); err != nil {
		return err
	}

	//append the the new one
	if err := this.appendElem(logIndex, buff); err != nil {
		return err
	}

	//update index file
	if err := this.updateLastIndex(uint64(1), logIndex, uint64(dataLen)); err != nil {
		return err
	}

	return nil
}

// get the last elem
func (this *diskIo) last() (uint64, []byte, error) {
	lastFileName := this.getLatestFileName()
	// if no data in the disk
	if lastFileName == "" {
		return uint64(0), nil, DISK_NOTFOUND_ERR
	}

	indexInfo := this.idxMgr.mapIndex[lastFileName]
	lastElemPos := indexInfo.meta.lastRecordPos
	lastFile := this.latestFilePtr

	startId, _, buff, err := getElemByPos(lastFile, lastElemPos)
	return startId, buff, err
}

// return startId, endId, buff
func (this *diskIo) get(id uint64) (uint64, uint64, []byte, error) {
	// find target file
	fileName := ""
	var indexInfo *indexInfo = nil
	for name, info := range this.idxMgr.mapIndex {
		if info.meta.minId <= id && id <= info.meta.maxId {
			fileName = name
			indexInfo = info
			break
		}
	}
	if fileName == "" || indexInfo == nil {
		return 0, 0, nil, DISK_NOTFOUND_ERR
	}

	// find the index pos
	startPos, endPos, err := indexInfo.findIndexPosById(id)
	if err != nil {
		return 0, 0, nil, err
	}
	//fmt.Println("find pos: id, start, end:", id, startPos, endPos)

	// get file pointer
	dataFile, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return 0, 0, nil, err
	}

	// read from disk
	startId, endId, buff, err := getElemByIdAndIndex(dataFile, id, startPos, endPos)
	if err != nil {
		return 0, 0, nil, err
	}

	return startId, endId, buff, nil
}

func (this *diskIo) listAfter(id uint64) ([]*diskElem, error) {
	result := make([]*diskElem, 0)

	// sort files by startId
	sorter := newIdxMgrSorter(this)
	sort.Sort(sorter)

	for _, it := range sorter.items {
		filename := it.fileName
		indexInfo := it.indexInfo
		if indexInfo.meta.minId <= id {
			if indexInfo.meta.maxId >= id || indexInfo.meta.maxId == 0 {
				//fmt.Println("part!!!!:", filename)
				// find the index pos
				startPos, _, err := indexInfo.findIndexPosById(id)
				if err != nil && err != DISK_NOTFOUND_ERR {
					return nil, err
				}

				// open the data file
				file, err := os.OpenFile(filename, os.O_RDONLY, 0)
				if err != nil {
					return nil, errors.New("OpenFile failed in listAfter:"+err.Error())
				}
				defer file.Close()
				//fmt.Println("test!! startPos, filename:", startPos, filename)
				elems, err := getElemsAfterIdByIndex(file, id, startPos)
				if err != nil {
					return nil, err
				}
				//fmt.Println("get:", len(elems))
				result = append(result, elems...)
				//fmt.Println("len is now:", len(result))
			}
		} else {
			//fmt.Println("total!!!!:", filename)

			// open the data file
			file, err := os.OpenFile(filename, os.O_RDONLY, 0)
			if err != nil {
				return nil, errors.New("OpenFile failed in listAfter:"+err.Error())
			}
			defer file.Close()
			//fmt.Println("test!! startPos, filename:", startPos, filename)
			elems, err := getElemsFromFile(file)
			if err != nil {
				return nil, err
			}
			//fmt.Println("get:", len(elems))
			result = append(result, elems...)
			//fmt.Println("len is now:", len(result))
		}
	}

	return result, nil
}

func (this *diskIo) listBetween(startId uint64, endId uint64) ([]*diskElem, error) {
	result := make([]*diskElem, 0)

	// sort files by startId
	sorter := newIdxMgrSorter(this)
	sort.Sort(sorter)

	for _, it := range sorter.items {
		filename := it.fileName
		indexInfo := it.indexInfo
		if indexInfo.meta.minId > endId || indexInfo.meta.maxId < startId {
			//fmt.Println("continue:", filename)
			continue
		} else {
			//totally read
			if indexInfo.meta.minId >= startId && indexInfo.meta.maxId <= endId {
				//fmt.Println("total!!!", filename)

				// open the data file
				file, err := os.OpenFile(filename, os.O_RDONLY, 0)
				if err != nil {
					return nil, errors.New("OpenFile failed in listAfter:"+err.Error())
				}
				defer file.Close()
				//fmt.Println("test!! startPos, filename:", startPos, filename)
				elems, err := getElemsFromFile(file)
				if err != nil {
					return nil, err
				}
				//fmt.Println("get:", len(elems))
				result = append(result, elems...)
				//fmt.Println("len is now:", len(result))
			} else {
				//fmt.Println("part!!!", filename)
				// find the index pos
				startIdStartPos, startIdEndPos, err := indexInfo.findIndexPosById(startId)
				if err != nil && err != DISK_NOTFOUND_ERR {
					return nil, err
				}

				endIdStartPos, endIdEndPos, err := indexInfo.findIndexPosById(endId)
				if err != nil && err != DISK_NOTFOUND_ERR {
					return nil, err
				}

				//fmt.Println(startIdStartPos, startIdEndPos, endIdStartPos, endIdEndPos)
				// open the data file
				file, err := os.OpenFile(filename, os.O_RDONLY, 0)
				if err != nil {
					return nil, errors.New("OpenFile failed in listAfter:"+err.Error())
				}
				defer file.Close()
				//fmt.Println("test!! startPos, filename:", startPos, filename)
				elems, err := getElemsBetweenIdByIndex(file, startId, endId, startIdStartPos, startIdEndPos, endIdStartPos, endIdEndPos)
				if err != nil {
					return nil, err
				}
				//fmt.Println("get:", len(elems))
				result = append(result, elems...)
				//fmt.Println("len is now:", len(result))
			}
		}
	}

	return result, nil
}

/*
	list all elems int the latest file
 */
func (this *diskIo) listOfLatestFile() ([]*diskElem, error) {
	if this.latestFilePtr == nil {
		return nil, nil
	}

	elems, err := getElemsAfterIdByIndex(this.latestFilePtr, 0, 0)
	if err != nil {
		return nil, err
	}

	return elems, nil
}

// truncateBefore id, result: [id, maxId]
func (this *diskIo) truncateBefore(id uint64) error {
	for fileName, indexInfo := range this.idxMgr.mapIndex {
		if indexInfo.meta.maxId < id && indexInfo.meta.maxId != 0 { // maxId < id, delete all records
			err := os.Remove(fileName)
			if err != nil {
				glog.Errorf("Remove %s failed:%s\n", fileName, err.Error())
				return err
			}

			// delete index file
			err = this.deleteIndexByFile(fileName)
			if err != nil {
				return err
			}
		} else if indexInfo.meta.minId >= id { // minId > id, keep all records
			continue
		} else { // delete front part, keeps [id, maxId]
			// get exactly pos
			elem, pos, err := this.getStartPosById(fileName, id)
			if err != nil {
				return err
			}
			//fmt.Println("getStartPosById, pos:", pos)

			// update the elem(it bacame the minimum elem)
			elem.startId = id

			newFileName, err := this.truncateFileBeforeId(fileName, id, elem, pos)
			if err != nil {
				return err
			}

			// delete old file
			err = os.Remove(fileName)
			if err != nil {
				return err
			}

			// delete old index
			err = this.deleteIndexByFile(fileName)
			if err != nil {
				return err
			}

			// build new index
			err = this.buildIndexByFile(newFileName)
			if err != nil {
				return err
			}
		}
	}

	err := this.updateLastFile()
	if err != nil {
		return err
	}

	return nil
}

/*
	truncate after id, result:[minId, id]
	@return *diskElem: the latest elem after truncate
 */
func (this *diskIo) truncateAfter(id uint64) error {
	//var resultElem *diskElem = nil
	for fileName, indexInfo := range this.idxMgr.mapIndex {
		if indexInfo.meta.minId > id { // minId > id, delete all records
			err := os.Remove(fileName)
			if err != nil {
				//return nil, err
				return err
			}

			// delete index file
			err = this.deleteIndexByFile(fileName)
			if err != nil {
				//return nil, err
				return err
			}
		} else if indexInfo.meta.maxId < id && indexInfo.meta.maxId != 0 { // maxId < id && maxId != 0, keep all records
			continue
		} else { // delete tail part, keeps [minId, id]
			// get exactly pos
			elem, pos, err := this.getStartPosById(fileName, id)
			if err != nil {
				//return nil, err
				return err
			}
			//resultElem = elem
			//fmt.Printf("getStartPosById, pos:%x\n", pos)
			//fmt.Println("elem is :", elem.startId, elem.endId, string(elem.buff))

			// update the elem's endId
			elem.endId = id

			err = this.truncateFileAfterElem(fileName, elem, pos)
			if err != nil {
				//return nil, err
				return err
			}

			// delete old index
			err = this.deleteIndexByFile(fileName)
			if err != nil {
				//return nil, err
				return err
			}

			// build new index
			err = this.buildIndexByFile(fileName)
			if err != nil {
				//return nil, err
				return err
			}
		}
	}

	err := this.updateLastFile()
	if err != nil {
		//return nil, err
		return err
	}

	//return resultElem, nil
	return nil
}

/********************* internal functions *************************************/

func (this *diskIo) updateLastFile() error {
	var latestStartId uint64 = 0
	var latestFileName string = ""
	for filename, _ := range this.idxMgr.mapIndex {
		id, err := this.getStartIdByFileName(filename)
		if err == nil {
			if id > latestStartId {
				latestStartId = id
				latestFileName = filename
			}
		}
	}

	//fmt.Println("new lastfile, this.lastfile:", latestFileName, this.latestFileName)

	// reopen latest file, no matter if it changes or not
	if latestFileName != "" {
		this.latestFileName = latestFileName

		this.latestFilePtr.Close()
		file, err := os.OpenFile(latestFileName, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		this.latestFilePtr = file
	}

	return nil
}

/*
	truncate before pos
	@return string: new filename
 */
func (this *diskIo) truncateFileBeforeId(fileName string, id uint64, elem *diskElem, pos uint64) (string, error) {
	newFileName := this.getFileNameByStartId(id)
	//fmt.Println("old filename, newfilename:", fileName, newFileName)

	oldFile, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer oldFile.Close()

	_, err = oldFile.Seek(int64(pos), 0)
	if err != nil {
		return "", err
	}

	newFile, err := os.Create(newFileName)
	if err != nil {
		return "", err
	}
	defer newFile.Close()

	//convert start elem to buffer
	elemBuff := make([]byte, DATA_BLOCK_SIZE)
	binary.BigEndian.PutUint64(elemBuff[DATA_STARTID_POS : DATA_STARTID_POS+ID_LEN], elem.startId)
	binary.BigEndian.PutUint64(elemBuff[DATA_ENDID_POS : DATA_ENDID_POS+ID_LEN], elem.endId)
	binary.BigEndian.PutUint64(elemBuff[DATA_BUFFLEN_POS : DATA_BUFFLEN_POS+SIZE_LEN], uint64(len(elem.buff)))
	copy(elemBuff[DATA_BUFF_POS : DATA_BUFF_POS+uint64(len(elem.buff))], elem.buff)

	// write to new file
	n, err := newFile.Write(elemBuff)
	if err != nil {
		return "", err
	} else if uint64(n) != DATA_BLOCK_SIZE {
		return "", errors.New("write elem failed")
	}

	// loop copy
	buff := make([]byte, IDX_MAX_SECTION_SIZE)
	for {
		// read
		nRead, rdErr := oldFile.Read(buff)
		if rdErr != nil && rdErr != io.EOF {
			return "", rdErr
		}

		// write
		nWrite, wrErr := newFile.Write(buff[0 : nRead])
		if wrErr != nil {
			return "", wrErr
		} else if nWrite != nRead {
			return "", errors.New(fmt.Sprintf("write file error, need to write %d, but actually write %d\n", nRead, nWrite))
		}

		// break if EOF
		if rdErr == io.EOF || uint64(nRead) != IDX_MAX_SECTION_SIZE {
			break
		}
	}

	return newFileName, nil
}

func (this *diskIo) truncateFileAfterElem(fileName string, elem *diskElem, pos uint64) error {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		return err
	}

	elemBuffLen := uint64(len(elem.buff))
	paddedSize := getPaddedSize(elemBuffLen)
	elemSize := DATA_HEAD_SIZE + elemBuffLen + paddedSize

	newSize := int64(pos + elemSize)
	err = file.Truncate(newSize)
	if err != nil {
		return err
	}

	return nil
}

func (this *diskIo) deleteIndexByFile(fileName string) error {
	indexInfo := this.idxMgr.mapIndex[fileName]
	indexInfo.filePtr.Close()

	indexFileName := dataFileNameToIdxFileName(fileName)
	os.Remove(indexFileName)

	delete(this.idxMgr.mapIndex, fileName)

	return nil
}

/*
	get the exactly start pos of elem in the disk by id and filename
	@return *diskEle: elem by id
	@return uint64: startPos of the elem
 */
func (this *diskIo) getStartPosById(filename string, id uint64) (*diskElem, uint64, error) {
	resultElem := &diskElem{}

	// get index pos
	indexInfo := this.idxMgr.mapIndex[filename]
	startPos, _, err := indexInfo.findIndexPosById(id)
	if err != nil {
		return nil, 0, err
	}

	// open data file
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		glog.Errorf("Remove %s failed:%s\n", filename, err.Error())
		return nil, 0, err
	}

	// search for the exactly pos of id
	exactlyPos := uint64(0)
	buff := make([]byte, IDX_MAX_SECTION_SIZE)
	n, err := file.ReadAt(buff, int64(startPos))
	if err != nil && err != io.EOF {
		return nil, 0, err
	}

	for readSize := uint64(0); readSize < uint64(n); {
		// read header
		startId := binary.BigEndian.Uint64(buff[readSize + DATA_STARTID_POS : readSize + DATA_STARTID_POS + ID_LEN])
		endID := binary.BigEndian.Uint64(buff[readSize + DATA_ENDID_POS : readSize + DATA_ENDID_POS + ID_LEN])
		buffLen := binary.BigEndian.Uint64(buff[readSize + DATA_BUFFLEN_POS : readSize + DATA_BUFFLEN_POS + SIZE_LEN])
		//fmt.Println("pos, startId, endId, bufflen:", readSize, startId, endID, buffLen)

		if startId <= id {
			if id <= endID || endID == 0 {
				// hit
				exactlyPos = readSize
				resultElem.startId = startId
				resultElem.endId = endID
				resultElem.buff = make([]byte, buffLen)
				copy(resultElem.buff, buff[readSize+DATA_BUFF_POS : readSize+DATA_BUFF_POS+buffLen])

				break
			}
		}

		paddedSize := getPaddedSize(buffLen)
		readSize += DATA_HEAD_SIZE+buffLen+paddedSize
	}

	return resultElem, startPos + exactlyPos, nil
}

/*
	when search elems between id[5, 10], the elem[3, 6] is included
 */
func getElemsBetweenIdByIndex(file *os.File, startId uint64, endId, startIdStartPos uint64, startIdEndPos uint64, endIdStartPos uint64, endIdEndPos uint64) ([]*diskElem, error) {
	if file == nil {
		return nil, errors.New("file ptr is nil in getElemsAfterIdByIndex")
	}

	// search for the exactly pos of startId
	startIdRangeSize := startIdEndPos - startIdStartPos
	startIdRangeBuff := make([]byte, startIdRangeSize)
	nStartBuffRead, err := file.ReadAt(startIdRangeBuff, int64(startIdStartPos))
	if err != nil && err != io.EOF {
		return nil, errors.New("file.ReadAt error in getElemsAfterIdByIndex:" + err.Error())
	}

	readSize := uint64(0)
	for ; readSize < uint64(nStartBuffRead); {
		// read header
		//elemStartId := binary.BigEndian.Uint64(startIdRangeBuff[readSize + DATA_STARTID_POS : readSize + DATA_STARTID_POS + ID_LEN])
		elemEndId := binary.BigEndian.Uint64(startIdRangeBuff[readSize + DATA_ENDID_POS : readSize + DATA_ENDID_POS + ID_LEN])
		buffLen := binary.BigEndian.Uint64(startIdRangeBuff[readSize + DATA_BUFFLEN_POS : readSize + DATA_BUFFLEN_POS + SIZE_LEN])
		//fmt.Println("pos, startId, endId, bufflen:", readSize, startId, endID, buffLen)

		if elemEndId >= startId || elemEndId == 0 {
			//fmt.Println("hit!!!!!!!!!!:", elemStartId, elemEndId, startId)
			// hit
			//startIdHitPos = readSize
			break
		}

		paddedSize := getPaddedSize(buffLen)
		readSize += DATA_HEAD_SIZE + buffLen+paddedSize
	}
	exactlyStartPos := startIdStartPos + readSize

	// search for the exactly pos of endId
	endIdRangeSize := endIdEndPos - endIdStartPos
	endIdRangeBuff := make([]byte, endIdRangeSize)
	nEndBuffRead, err := file.ReadAt(endIdRangeBuff, int64(endIdStartPos))
	if err != nil && err != io.EOF {
		return nil, errors.New("file.ReadAt error in getElemsAfterIdByIndex:" + err.Error())
	}

	readSize = uint64(0)
	for ; readSize < uint64(nEndBuffRead); {
		// read header
		elemStartId := binary.BigEndian.Uint64(endIdRangeBuff[readSize + DATA_STARTID_POS : readSize + DATA_STARTID_POS + ID_LEN])
		//elemEndId := binary.BigEndian.Uint64(endIdRangeBuff[readSize + DATA_ENDID_POS : readSize + DATA_ENDID_POS + ID_LEN])
		buffLen := binary.BigEndian.Uint64(endIdRangeBuff[readSize + DATA_BUFFLEN_POS : readSize + DATA_BUFFLEN_POS + SIZE_LEN])
		//fmt.Println("pos, startId, endId, bufflen:", readSize, startId, endID, buffLen)

		if elemStartId > endId {
			//fmt.Println("hit!!!!!!!!!!:", startId, endId, id)
			// hit
			//endIdHitPos = readSize
			break
		}

		paddedSize := getPaddedSize(buffLen)
		readSize += DATA_HEAD_SIZE + buffLen + paddedSize
	}
	exactlyEndPos := endIdStartPos + readSize

	// count the total size
	totalSize := exactlyEndPos - exactlyStartPos

	if totalSize == 0 {
		return make([]*diskElem, 0), nil
	}

	// make a big enough buff to load all needed data from disk
	allBuff := make([]byte, totalSize)
	nReadAll, err := file.ReadAt(allBuff, int64(exactlyStartPos))
	if err != nil {
		return nil, errors.New("file.ReadAt error:" + err.Error())
	}

	// parse to elements
	return getElemsFromBuff(allBuff[0 : nReadAll])
}

/*
	get elements bigger than the id provided, with the help of an index pos 
 */
func getElemsAfterIdByIndex(file *os.File, id uint64, startPos uint64) ([]*diskElem, error) {
	if file == nil {
		return nil, errors.New("file ptr is nil in getElemsAfterIdByIndex")
	}

	// search for the exactly pos of id
	hitPos := uint64(0)
	buff := make([]byte, IDX_MAX_SECTION_SIZE)
	n, err := file.ReadAt(buff, int64(startPos))
	if err != nil && err != io.EOF {
		return nil, errors.New("file.ReadAt error in getElemsAfterIdByIndex:"+err.Error())
	}

	for readSize := uint64(0); readSize < uint64(n); {
		// read header
		startId := binary.BigEndian.Uint64(buff[readSize + DATA_STARTID_POS : readSize + DATA_STARTID_POS + ID_LEN])
		endId := binary.BigEndian.Uint64(buff[readSize + DATA_ENDID_POS : readSize + DATA_ENDID_POS + ID_LEN])
		buffLen := binary.BigEndian.Uint64(buff[readSize + DATA_BUFFLEN_POS : readSize + DATA_BUFFLEN_POS + SIZE_LEN])
		//fmt.Println("pos, startId, endId, bufflen:", readSize, startId, endID, buffLen)

		if startId <= id {
			if id <= endId || endId == 0 {
				//fmt.Println("hit!!!!!!!!!!:", startId, endId, id)
				// hit
				hitPos = readSize
				break
			}
		}

		paddedSize := getPaddedSize(buffLen)
		readSize += DATA_HEAD_SIZE+buffLen+paddedSize
	}
	exactlyPos := startPos + hitPos
	//fmt.Println("exactlypos:", exactlyPos)

	// make a big enough buff to load all needed data from disk
	// todo (if the buff is too big to make, consider to batches read from disk)
	info, err := file.Stat()
	if err != nil {
		return nil, errors.New("file.Stat error:"+err.Error())
	}
	allBuffSize := uint64(info.Size()) - exactlyPos
	var allBuff []byte
	if allBuffSize <= uint64(n) {
		allBuff = buff[hitPos : n]
	} else {
		allBuff = make([]byte, allBuffSize)

		// in order to avoid from read the same data from disk twice, reuse them
		reuseSize := uint64(n) - exactlyPos
		//fmt.Println("exactlypos, reusesize, buffsize", exactlyPos, reuseSize, allBuffSize)
		copy(allBuff[0 : reuseSize], buff[hitPos : n])
		_, err = file.ReadAt(allBuff[reuseSize : allBuffSize], int64(n))
		if err != nil {
			return nil, err
		}
	}

	// parse to elements
	return getElemsFromBuff(allBuff)
}

func getElemsFromBuff(buff []byte) ([]*diskElem, error) {
	buffLen := uint64(len(buff))
	result := make([]*diskElem, 0)
	for readSize := uint64(0); readSize < uint64(buffLen); {
		// read header
		startId := binary.BigEndian.Uint64(buff[readSize + DATA_STARTID_POS : readSize + DATA_STARTID_POS + ID_LEN])
		endID := binary.BigEndian.Uint64(buff[readSize + DATA_ENDID_POS : readSize + DATA_ENDID_POS + ID_LEN])
		elemBuffLen := binary.BigEndian.Uint64(buff[readSize + DATA_BUFFLEN_POS : readSize + DATA_BUFFLEN_POS + SIZE_LEN])
		//fmt.Println("pos, startId, endId, elemBuffLen, buffLen:", readSize, startId, endID, elemBuffLen, buffLen)

		elem := &diskElem {
			startId: startId,
			endId: endID,
			buff: make([]byte, elemBuffLen),
		}
		copy(elem.buff, buff[readSize + DATA_BUFF_POS : readSize + DATA_BUFF_POS + elemBuffLen])
		result = append(result, elem)

		paddedSize := getPaddedSize(elemBuffLen)
		readSize += DATA_HEAD_SIZE + elemBuffLen + paddedSize
	}

	//fmt.Printf("get %d elems\n", len(result))
	return result, nil
}

/*
	search the index to find an inexact pos of id
	@return startPos, endPos, error
 */
func (this *indexInfo) findIndexPosById(id uint64) (uint64, uint64, error) {
	// if id is between minId and the first index
	if this.meta.minId <= id && this.indexs[0].startId > id {
		return 0, this.indexs[0].pos, nil
	}

	indexNum := len(this.indexs)
	for i := 0; i < indexNum; i++ {
		// records are sorted from small to large, so if this elem is larger than the need, the next one will be even larger
		if this.indexs[i].startId > id {
			break
		}

		// the last index
		if i >= indexNum-1 {
			if this.indexs[i].startId <= id {
				return this.indexs[i].pos, this.meta.dataFileSize, nil
			}

			break
		}

		//fmt.Println("this.indexs[i].startId, this.indexs[i+1].startId", this.indexs[i].startId, this.indexs[i+1].startId)
		if this.indexs[i].startId < id && this.indexs[i + 1].startId > id {
			return this.indexs[i].pos, this.indexs[i + 1].pos, nil
		} else if this.indexs[i].startId == id {
			return this.indexs[i].pos, this.indexs[i].pos, nil
		}
	}

	return 0, 0, DISK_NOTFOUND_ERR
}

/*
	get all elems from the data file specified
 */
func getElemsFromFile(file *os.File) ([]*diskElem, error) {
	if file == nil {
		return nil, errors.New("file ptr is nil in getElemByPos")
	}

	info, err := file.Stat()
	if err != nil {
		return nil, errors.New("file.Stat failed in getElemsFromFile:" + err.Error())
	}

	buffSize := info.Size()
	buff := make([]byte, buffSize)
	n, err := file.ReadAt(buff, 0)
	if err != nil {
		return nil, errors.New("file.ReadAt failed in getElemsFromFile:" + err.Error())
	} else if n != int(buffSize) {
		return nil, errors.New("file.ReadAt failed in getElemsFromFile:not read enough bytes\n")
	}

	// parse to elements
	return getElemsFromBuff(buff[0 : n])
}


/*
 get an elem by pos
 @param file: pointer to the file
 @param pos: position of the elem
 @return startId, endId, buff, error: nothing to tell
  */
func getElemByPos(file *os.File, pos uint64) (uint64, uint64, []byte, error) {
	if file == nil {
		return 0, 0, nil, errors.New("file ptr is nil in getElemByPos")
	}

	// read a block
	buff := make([]byte, DATA_BLOCK_SIZE)
	_, err := file.ReadAt(buff, int64(pos))
	if err != nil {
		return uint64(0), uint64(0), nil, err
	}

	// get meta
	startId := binary.BigEndian.Uint64(buff[DATA_STARTID_POS : DATA_STARTID_POS + ID_LEN])
	endID := binary.BigEndian.Uint64(buff[DATA_ENDID_POS : DATA_ENDID_POS + ID_LEN])
	buffLen := binary.BigEndian.Uint64(buff[DATA_BUFFLEN_POS : DATA_BUFFLEN_POS + SIZE_LEN])

	// read rest parts when the elem is more than one block
	if buffLen+DATA_HEAD_SIZE > DATA_BLOCK_SIZE {
		moreSize := buffLen + DATA_HEAD_SIZE - DATA_BLOCK_SIZE
		moreBuff := make([]byte, moreSize)
		_, err := file.ReadAt(moreBuff, int64(pos + DATA_BLOCK_SIZE))
		if err != nil {
			return uint64(0), uint64(0), nil, err
		}

		buff = append(buff, moreBuff...)
	}
	return startId, endID, buff[DATA_HEAD_SIZE : DATA_HEAD_SIZE+buffLen], nil
}

/*
 get an elem by pos range
 @param file: pointer to the file
 @param pos: position of the elem
 @return startId, endId, buff, error: nothing to tell
  */
func getElemByIdAndIndex(file *os.File, id uint64, startPos uint64, endPos uint64) (uint64, uint64, []byte, error) {
	if file == nil {
		return 0, 0, nil, errors.New("file ptr is nil in getElemByIdAndIndex")
	}

	//fmt.Println("start getElemByIdAndIndex: startPos, endPos, id", startPos, endPos, id)
	// read a section
	sectionSize := endPos - startPos
	sectionBuff := make([]byte, sectionSize)

	n, err := file.ReadAt(sectionBuff, int64(startPos))
	if err != nil {
		if err == io.EOF {
			sectionBuff = sectionBuff[ : n]
			sectionSize = uint64(n)
		} else {
			return 0, 0, nil, err
		}
	}

	for readSize := uint64(0); readSize < sectionSize; {
		// read header
		startId := binary.BigEndian.Uint64(sectionBuff[readSize + DATA_STARTID_POS : readSize + DATA_STARTID_POS + ID_LEN])
		endId := binary.BigEndian.Uint64(sectionBuff[readSize + DATA_ENDID_POS : readSize + DATA_ENDID_POS + ID_LEN])
		buffLen := binary.BigEndian.Uint64(sectionBuff[readSize + DATA_BUFFLEN_POS : readSize + DATA_BUFFLEN_POS + SIZE_LEN])
		//fmt.Println("pos, startId, endId, bufflen:", readSize, startId, endId, buffLen)

		if startId <= id {
			if id <= endId || endId == 0 {
				// hit
				return startId, endId, sectionBuff[readSize+DATA_BUFF_POS : readSize+DATA_BUFF_POS+buffLen], nil
			}
		}

		paddedSize := getPaddedSize(buffLen)

		readSize += DATA_HEAD_SIZE+buffLen+paddedSize
	}

	return 0, 0, nil, errors.New(fmt.Sprintf("getElemByPosRange failed, id:%d, startPos:%d, endPos %d\n", id, startPos, endPos))
}

func (this *diskIo) init() error {
	// mkdir, nothing changed if path already exists
	err := os.MkdirAll(this.path, 0777)
	if err != nil {
		return err
	}

	// scan all data files
	pattern := filepath.Join(this.path, "*.data")
	//fmt.Println("pattern:", pattern)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	//fmt.Println("files", files)

	var maxStartId uint64 = 0
	lastFileName := ""
	for _, filename := range files {
		// find the latest file
		startId, err := this.getStartIdByFileName(filename)
		if err != nil {
			return err
		}
		if startId > maxStartId {
			maxStartId = startId
			lastFileName = filename

		}

		// load index files
		err = this.loadIndex(filename)
		if err != nil {
			return err
		}
	}

	// open last file
	if lastFileName != "" {
		lastFile, err := os.Open(lastFileName)
		if err != nil {
			return err
		}
		this.latestFileName = lastFileName
		this.latestFilePtr = lastFile
	}

	return nil
}

func (this *diskIo) loadIndex(dataFileName string) error {
	// get index filename by data filename
	indexFileName := dataFileNameToIdxFileName(dataFileName)

	_, err := os.Stat(indexFileName)
	if err != nil {
		//if not exists
		if os.IsNotExist(err) {
			if err := this.buildIndexByFile(dataFileName); err != nil {
				return err
			}

			// write index file to disk
			indexInfo := this.idxMgr.mapIndex[dataFileName]
			if err := indexInfo.writeIndexToDisk(); err != nil {
				return err
			}

			return nil
		}

		return err
	}

	//if index file exists, load it
	return this.readIndex(indexFileName)
}

func (this *indexInfo) writeIndexToDisk() error {
	file := this.filePtr
	// count for buff size
	buffSize := IDX_HEADER_SIZE + uint64(len(this.indexs)) * SI_SIZE
	buff := make([]byte, buffSize)
	binary.BigEndian.PutUint64(buff[IDX_DATAFILESIZE_POS : IDX_DATAFILESIZE_POS+SIZE_LEN], this.meta.dataFileSize)
	binary.BigEndian.PutUint64(buff[IDX_RECORDNUM_POS : IDX_RECORDNUM_POS+NUM_LEN], this.meta.recordNum)
	binary.BigEndian.PutUint64(buff[IDX_LASTRECORDPOS_POS : IDX_LASTRECORDPOS_POS+POS_LEN], this.meta.lastRecordPos)
	binary.BigEndian.PutUint64(buff[IDX_MINID_POS : IDX_MINID_POS+ID_LEN], this.meta.minId)
	binary.BigEndian.PutUint64(buff[IDX_MAXID_POS : IDX_MAXID_POS+ID_LEN], this.meta.maxId)

	nowPos := IDX_HEADER_SIZE
	for _, idx := range this.indexs {
		binary.BigEndian.PutUint64(buff[nowPos+SI_STARTID_POS : nowPos+SI_STARTID_POS+ID_LEN], idx.startId)
		binary.BigEndian.PutUint64(buff[nowPos+SI_POS_POS : nowPos+SI_POS_POS+POS_LEN], idx.pos)
		nowPos += SI_SIZE
	}

	n, err := file.WriteAt(buff, 0)
	if err != nil {
		return err
	} else if uint64(n) < buffSize {
		return errors.New("write index file failed: not write completely")
	}

	return nil
}

func (this *indexInfo) writeMetaToDisk() error {
	// insure that index file exists


	//fmt.Println("water!", this.waterLevel)
	file := this.filePtr
	// count for buff size
	buffSize := IDX_HEADER_SIZE
	buff := make([]byte, buffSize)
	binary.BigEndian.PutUint64(buff[IDX_DATAFILESIZE_POS : IDX_DATAFILESIZE_POS+SIZE_LEN], this.meta.dataFileSize)
	binary.BigEndian.PutUint64(buff[IDX_RECORDNUM_POS : IDX_RECORDNUM_POS+NUM_LEN], this.meta.recordNum)
	binary.BigEndian.PutUint64(buff[IDX_LASTRECORDPOS_POS : IDX_LASTRECORDPOS_POS+POS_LEN], this.meta.lastRecordPos)
	binary.BigEndian.PutUint64(buff[IDX_MINID_POS : IDX_MINID_POS+ID_LEN], this.meta.minId)
	binary.BigEndian.PutUint64(buff[IDX_MAXID_POS : IDX_MAXID_POS+ID_LEN], this.meta.maxId)
	binary.BigEndian.PutUint64(buff[IDX_RECORDLEVEL_POS : IDX_RECORDLEVEL_POS+NUM_LEN], this.waterLevel.recordCount)
	binary.BigEndian.PutUint64(buff[IDX_SIZELEVEL_POS : IDX_SIZELEVEL_POS+SIZE_LEN], this.waterLevel.sizeCount)
	//fmt.Println("pos:", IDX_RECORDLEVEL_POS, IDX_SIZELEVEL_POS, this.waterLevel.recordCount, this.waterLevel.sizeCount)
	n, err := file.WriteAt(buff, 0)
	if err != nil {
		return err
	} else if uint64(n) < buffSize {
		return errors.New("write index file failed: not write completely")
	}

	return nil
}

func (this *diskIo) buildIndexByFile(dataFileName string) error {
	indexFileName := dataFileNameToIdxFileName(dataFileName)
	if err := this.createNewIndex(indexFileName); err != nil {
		return err
	}

	//open data file
	dataFile, err := os.Open(dataFileName)
	if err != nil {
		glog.Errorf("open %s failed:%s\n", dataFileName, err.Error())
		return err
	}
	defer dataFile.Close()

	// cycle read data file, 1MB a time, till read a EOF
	buffSize := IDX_MAX_SECTION_SIZE
	buff := make([]byte, buffSize)
	restBuff := make([]byte, buffSize)
	restBuffLen := 0
	haveRestBuff := false
	for {
		n , err := dataFile.Read(buff)
		if err != nil && err != io.EOF {
			glog.Errorf("read %s failed:%s\n", dataFileName, err.Error())
			return err
		}

		if haveRestBuff {
			buff = append(restBuff[0 : restBuffLen], buff...)
			haveRestBuff = false
			restBuffLen = 0
		}

		indexInfo := this.idxMgr.mapIndex[dataFileName]
		rest, err := indexInfo.buildIndexByFileBuff(buff[0 : n])
		if err != nil {
			if strings.Contains(err.Error(), "need more blocks") {
				if err == io.EOF {
					return errors.New(string("data file is imcomplete:") + err.Error())
				} else if uint64(n) == buffSize {
					copy(restBuff, rest)
					restBuffLen = len(rest)
					haveRestBuff = true
					continue
				}

				return err
			}

			return err
		}

		if uint64(n) < buffSize {
			break
		}
	}

	return nil
}

//[record] = start_id(8 byte)end_id(8byte)buff_len(8 byte)buff(buff_len byte) 0 0 0 0 0 (expand to 512 bytes or n * 512 bytes)
// cycle read records, the tail may not be a complete record, return this unhandled buff for the next read
func (this *indexInfo) buildIndexByFileBuff(buff []byte) ([]byte, error) {
	buffLen := uint64(len(buff))
	for nowPos := uint64(0); nowPos < buffLen; {
		// read meta
		startId := binary.BigEndian.Uint64(buff[nowPos + DATA_STARTID_POS : nowPos + DATA_STARTID_POS + ID_LEN])
		//endId := binary.BigEndian.Uint64(buff[nowPos + DATA_ENDID_POS : nowPos + DATA_ENDID_POS + ID_LEN])
		dataBuffLen := binary.BigEndian.Uint64(buff[nowPos + DATA_BUFFLEN_POS : nowPos + DATA_BUFFLEN_POS + SIZE_LEN])

		// get padded size
		paddedSize := getPaddedSize(dataBuffLen)

		// finish read and return the unread part of buff if the remain size is less than the data buff len
		totalLen := nowPos + DATA_HEAD_SIZE + dataBuffLen + paddedSize
		recordSize := DATA_HEAD_SIZE + dataBuffLen + paddedSize
		//fmt.Println("nowPos startId, dataBuffLen, paddedSize, totalLen",nowPos, startId, dataBuffLen, paddedSize, totalLen)
		if totalLen > buffLen {
			return buff[nowPos : ], errors.New(fmt.Sprintf("need more blocks:totalLen %d, buffLen %d", totalLen, buffLen))
		}

		// update index for each record
		err := this.updateIndex(uint64(1), startId, recordSize)
		if err != nil {
			return nil, err
		}

		nowPos += recordSize
	}

	return nil, nil
}

func (this *diskIo) createNewIndex(indexFileName string) error {
	newIndexFile, err := os.Create(indexFileName)
	if err != nil {
		glog.Errorf("create %s failed:%s\n", indexFileName, err.Error())
		return err
	}

	indexInfo := &indexInfo{
		filePtr: newIndexFile,
		meta: fileMeta{},
		indexs: make([]*indexElem, 0),
		waterLevel: waterLevelInfo{},
	}
	dataFileName := idxFileNameToDataFileName(indexFileName)
	this.idxMgr.mapIndex[dataFileName] = indexInfo

	return nil
}

func (this *diskIo) readIndex(indexFileName string) error {
	idxFile, err := os.OpenFile(indexFileName, os.O_RDWR, 0)
	if err != nil {
		glog.Errorf("open %s failed:%s\n", indexFileName, err.Error())
		return err
	}

	dataFileName := idxFileNameToDataFileName(indexFileName)

	buff, err := readFileAll(idxFile)
	if err != nil {
		return err
	}

	// for meta
	//[data_file_size(8 byte)][record_num(4 byte)][last_record_pos(8 byte)][minId(8 byte)][maxId(8 byte)][record_num_level(4 byte)][size_level
	dataFileSize := binary.BigEndian.Uint64(buff[IDX_DATAFILESIZE_POS : IDX_DATAFILESIZE_POS + SIZE_LEN])
	recordNum := binary.BigEndian.Uint64(buff[IDX_RECORDNUM_POS : IDX_RECORDNUM_POS + NUM_LEN])
	lastRecordPos := binary.BigEndian.Uint64(buff[IDX_LASTRECORDPOS_POS : IDX_LASTRECORDPOS_POS + POS_LEN])
	minId := binary.BigEndian.Uint64(buff[IDX_MINID_POS : IDX_MINID_POS + ID_LEN])
	maxId := binary.BigEndian.Uint64(buff[IDX_MAXID_POS : IDX_MAXID_POS + ID_LEN])
	recordNumLevel := binary.BigEndian.Uint64(buff[IDX_RECORDLEVEL_POS : IDX_RECORDLEVEL_POS + NUM_LEN])
	recordSizeLevel := binary.BigEndian.Uint64(buff[IDX_SIZELEVEL_POS : IDX_SIZELEVEL_POS + SIZE_LEN])
	meta := fileMeta{
		dataFileSize: dataFileSize,
		recordNum: recordNum,
		lastRecordPos: lastRecordPos,
		minId: minId,
		maxId:maxId,
	}
	waterLevel := waterLevelInfo{
		recordCount: recordNumLevel,
		sizeCount: recordSizeLevel,
	}

	// range sections
	//[section_index] = start_id(8 byte)pos(8 byte) // save the start position of the first record in each section
	indexs := make([]*indexElem, 0)
	idxLen := uint64(len(buff))
	for nowPos := IDX_HEADER_SIZE; nowPos < idxLen; {
		startId := binary.BigEndian.Uint64(buff[nowPos + SI_STARTID_POS : nowPos + SI_STARTID_POS + ID_LEN])
		pos := binary.BigEndian.Uint64(buff[nowPos + SI_POS_POS : nowPos + SI_POS_POS + POS_LEN])
		indexElem := &indexElem{
			startId: startId,
			pos:pos,
		}
		indexs = append(indexs, indexElem)
		nowPos += SI_SIZE
	}
	//fmt.Println("meta", meta)
	//fmt.Println("waterlevel:", waterLevel)
	//for _, idx := range indexs {
	//fmt.Println("idx:", idx)
	//}


	indexInfo := &indexInfo {
		filePtr: idxFile,
		meta: meta,
		indexs: indexs,
		waterLevel: waterLevel,
	}
	this.idxMgr.mapIndex[dataFileName] = indexInfo

	return nil
}

func (this *diskIo) getLatestFileName() string {
	if this.latestFileName == "" {
		var latestStartId uint64 = 0
		for filename, _ := range this.idxMgr.mapIndex {
			id, err := this.getStartIdByFileName(filename)
			if err == nil {
				if id > latestStartId {
					latestStartId = id
					this.latestFileName = filename
				}
			}
		}
	}

	return this.latestFileName
}

// get the last data file to write, if it is full or no data files found, create a new one
func (this *diskIo) getLatestFileToWrite(id uint64, buffLen uint64) error {
	filename := this.getLatestFileName()
	if this.latestFilePtr == nil {
		// no data file exists
		if filename == "" {
			if err := this.createNewDataFile(id); err != nil {
				return err
			}

			return nil
		} else { // file exists, open it
			file, err := os.OpenFile(filename, os.O_RDWR, 0)
			if err != nil {
				glog.Errorf("open %s failed:%s\n", filename, err.Error())
				return err
			}

			this.latestFilePtr = file
		}
	}

	// check size of the file, if exceed the DATA_MAX_FILE_SIZE, open a new data file for write
	writeSize := buffLen + DATA_HEAD_SIZE
	indexInfo := this.idxMgr.mapIndex[filename]
	if indexInfo.meta.dataFileSize+writeSize > DATA_MAX_FILE_SIZE {
		// update maxId of the last file
		this.idxMgr.mapIndex[filename].meta.maxId = id-1

		// close the last file
		this.latestFilePtr.Close()

		// open a new one
		if err := this.createNewDataFile(id); err != nil {
			return err
		}

		return nil
	}

	return nil
}

// create a new data file by startId (when no data files found or the latest file is full)
func (this *diskIo) createNewDataFile(id uint64) error {
	// generate filename
	filename := this.getFileNameByStartId(id)

	// create new file
	file, err := os.Create(filename)
	if err != nil {
		return errors.New("createNewDataFile failed:" + err.Error())
	}
	this.latestFileName = filename
	this.latestFilePtr = file

	// create new index file
	idxFileName := dataFileNameToIdxFileName(filename)
	//idxFileName := fmt.Sprintf("%s_%0*d.idx", this.header, FILE_NAME_NUMLEN, id)
	// idxFileName = filepath.Join(this.path, idxFileName)
	idxFile, err := os.Create(idxFileName)
	if err != nil {
		glog.Errorf("create %s failed:%s\n", idxFileName, err.Error())
		return err
	}
	indexInfo := &indexInfo{
		filePtr: idxFile,
		meta: fileMeta{},
		indexs: make([]*indexElem, 0),
		waterLevel: waterLevelInfo{},
	}
	this.idxMgr.mapIndex[filename] = indexInfo

	return nil
}

func (this *diskIo) checkIdValid(startId uint64) error {
	lastFileName := this.getLatestFileName()
	indexInfo := this.idxMgr.mapIndex[lastFileName]

	// get file of the last elem
	lastFile, err := this.getLatestFilePtr()
	if err != nil {
		return nil
	}

	// the target file may be empty, this indicates that the elem is the first customer in our system, please be good to her
	if lastFile == nil {
		return nil
	}

	// getpos
	lastPos := indexInfo.meta.lastRecordPos

	// read startId of last elem
	idBuff := make([]byte, ID_LEN)
	_, err = lastFile.ReadAt(idBuff, int64(lastPos))
	if err != nil {
		glog.Errorf("read lastfile failed:%s\n", err.Error())
		return err
	}

	lastStartId := binary.BigEndian.Uint64(idBuff)
	if startId <= lastStartId {
		return errors.New("id is less than the last startId\n")
	}

	return nil
}

// get the file pointer of the last elem
func (this *diskIo) getLatestFilePtr() (*os.File, error) {
	filename := this.getLatestFileName()
	if this.latestFilePtr == nil {
		// no data file exists
		if filename == "" {
			return nil, nil
		} else { // file exists, open it
			file, err := os.OpenFile(filename, os.O_RDWR, 0)
			if err != nil {
				return nil, err
			}

			this.latestFilePtr = file
		}
	}

	return this.latestFilePtr, nil
}

// set the endId of the last elem
func (this *diskIo) updateLastElem(startId uint64) error {
	// get lastFile ptr
	lastFile := this.latestFilePtr

	// if file ptr is nil, means that there is not any elements already exists, nothing todo
	if lastFile == nil {
		return nil
	}

	// if file exist, we need to update the endId of its last elem
	// getpos
	lastFileName := this.getLatestFileName()
	idxInfo := this.idxMgr.mapIndex[lastFileName]
	lastPos := idxInfo.meta.lastRecordPos + DATA_ENDID_POS

	// write to file
	idBuff := make([]byte, ID_LEN)
	endId := startId - 1
	binary.BigEndian.PutUint64(idBuff, endId)
	_, err := lastFile.WriteAt(idBuff, int64(lastPos))
	if err != nil {
		glog.Errorf("write last file failed:%s\n", err.Error())
		return err
	}

	return nil
}

func (this *diskIo) appendElem(startId uint64, buff []byte) error {
	file := this.latestFilePtr

	buffLen := uint64(len(buff))
	headerSize := DATA_HEAD_SIZE
	headerBuff := make([]byte, headerSize)

	binary.BigEndian.PutUint64(headerBuff[DATA_STARTID_POS : DATA_STARTID_POS+ID_LEN], startId)
	binary.BigEndian.PutUint64(headerBuff[DATA_BUFFLEN_POS : DATA_BUFFLEN_POS+SIZE_LEN], buffLen)

	lastFileName := this.getLatestFileName()
	dataFileSize := this.idxMgr.mapIndex[lastFileName].meta.dataFileSize

	n, err := file.WriteAt(headerBuff, int64(dataFileSize))
	if err != nil {
		return err
	} else if n < int(headerSize) {
		return errors.New("write new record to data file failed: header not write completely\n")
	}

	n, err = file.WriteAt(buff, int64(dataFileSize)+int64(headerSize))
	if err != nil {
		return err
	} else if n < int(buffLen) {
		return errors.New(fmt.Sprintf("write new record to data file failed: buff not write completely, written %d, datasize:%d\n", n, int(buffLen)))
	}

	// each block not full will be padded with 0 at the tail
	paddedSize := getPaddedSize(buffLen)
	paddedBuff := make([]byte, paddedSize)
	n, err = file.WriteAt(paddedBuff, int64(dataFileSize + headerSize + buffLen))
	if err != nil {
		return err
	} else if n < int(paddedSize) {
		return errors.New(fmt.Sprintf("write padded 0 to data file failed: buff not write completely, written %d, padded datasize:%d\n", n, int(paddedSize)))
	}

	return nil
}

func (this *diskIo) getLastElemPos() (uint64, error) {
	lastFileName := this.getLatestFileName()
	lastFileIdxInfo := this.idxMgr.mapIndex[lastFileName]
	if lastFileIdxInfo == nil {
		return 0, errors.New(fmt.Sprintf("count not find index info for data file %s\n", lastFileName))
	}

	return lastFileIdxInfo.meta.lastRecordPos, nil
}

func (this *diskIo) getLatestElem() (uint64, []byte, error) {
	lastFileName := this.getLatestFileName()
	indexInfo := this.idxMgr.mapIndex[lastFileName]
	lastPos := indexInfo.meta.lastRecordPos
	lastFile := this.latestFilePtr

	// read a block
	buff := make([]byte, DATA_BLOCK_SIZE)
	_, err := lastFile.ReadAt(buff, int64(lastPos))
	if err != nil {
		return 0, nil, err
	}

	// read header
	startId := binary.BigEndian.Uint64(buff[DATA_STARTID_POS : DATA_STARTID_POS + ID_LEN])
	//endId := binary.BigEndian.Uint64(buff[DATA_ENDID_POS : DATA_ENDID_POS + ID_LEN])
	buffLen := binary.BigEndian.Uint64(buff[DATA_BUFFLEN_POS : DATA_BUFFLEN_POS + SIZE_LEN])

	// if one block is enough, return it
	if buffLen+DATA_HEAD_SIZE <= DATA_BLOCK_SIZE {
		return startId, buff[DATA_BUFF_POS : DATA_BUFF_POS+buffLen], nil
	} else {
		newBuff := make([]byte, buffLen)
		// copy the first block
		copy(newBuff, buff[DATA_BUFF_POS : DATA_BLOCK_SIZE])

		// copy others
		nowPos := DATA_BLOCK_SIZE - DATA_HEAD_SIZE
		_, err := lastFile.ReadAt(buff[nowPos:buffLen], int64(lastPos)+int64(DATA_BLOCK_SIZE))
		if err != nil {
			return 0, nil, err
		}

		return startId, newBuff, nil
	}
}

func (this *diskIo) updateLastIndex(count uint64, startId uint64, buffLen uint64) error {
	lastFileName := this.getLatestFileName()

	addSize := DATA_HEAD_SIZE + buffLen + getPaddedSize(buffLen)
	return this.updateIndex(lastFileName, count, startId, addSize)
}

func (this *diskIo) updateIndex(filename string, count uint64, startId uint64, size uint64) error {
	// find indexInfo by filename
	indexInfo := this.idxMgr.mapIndex[filename]
	return indexInfo.updateIndex(count, startId, size)
}

// update index.
// new index? ( sum > n or size > maxsize?)
func (this *indexInfo) updateIndex(count uint64, startId uint64, size uint64) error {
	//fmt.Println("updateIndex id, size", startId, size)
	if count != 1 {
		return errors.New("only append one record per time is supported now")
	}

	if this.meta.recordNum == 0 {
		this.meta.minId = startId
	}
	this.meta.maxId = startId
	this.meta.recordNum += count
	this.meta.lastRecordPos = this.meta.dataFileSize
	this.meta.dataFileSize += size

	this.waterLevel.recordCount += count
	this.waterLevel.sizeCount += size

	if this.waterLevel.recordCount > IDX_MAX_RECORD_PER_SECTION ||
		this.waterLevel.sizeCount > IDX_MAX_SECTION_SIZE ||
		this.waterLevel.recordCount == 1 {
		// add new index
		if err := this.addNewIndex(startId, this.meta.lastRecordPos); err != nil {
			return err
		}

		//update level
		this.waterLevel.recordCount = count
		this.waterLevel.sizeCount = size
	}

	return nil
}

func (this *indexInfo) addNewIndex(startId uint64, pos uint64) error {
	//fmt.Println("addNewIndex id, pos", startId, pos)
	idx := &indexElem {
		startId: startId,
		pos: pos,
	}
	indexTailPos := IDX_HEADER_SIZE + uint64(len(this.indexs)) * SI_SIZE

	// write to disk
	file := this.filePtr
	buff := make([]byte, SI_SIZE)
	binary.BigEndian.PutUint64(buff[SI_STARTID_POS : SI_STARTID_POS+ID_LEN], startId)
	binary.BigEndian.PutUint64(buff[SI_POS_POS : SI_POS_POS+POS_LEN], pos)
	n, err := file.WriteAt(buff, int64(indexTailPos))
	if err != nil {
		return err
	} else if uint64(n) < SI_SIZE {
		return errors.New("add new index to disk failed: not write completely")
	}

	// write to memory
	this.indexs = append(this.indexs, idx)

	return nil
}

func (this *diskIo) getStartIdByFileName(fileName string) (uint64, error) {
	fileName = filepath.Base(fileName)
	fileName = strings.TrimLeft(fileName, this.header+"_")
	fileName = strings.TrimRight(fileName, ".data")

	// check num len
	if len(fileName) != FILE_NAME_NUMLEN {
		return 0, errors.New("illegal data filename")
	}

	i, _ := strconv.ParseInt(fileName, 10, 64)
	return uint64(i), nil
}

func (this *diskIo) getFileNameByStartId(id uint64) string {
	// generate filename
	filename := fmt.Sprintf("%s_%0*d.data", this.header, FILE_NAME_NUMLEN, id)
	filename = filepath.Join(this.path, filename)

	return filename
}

func dataFileNameToIdxFileName(dataFileName string) string {
	return strings.TrimRight(dataFileName, ".data") + ".idx"
}

func idxFileNameToDataFileName(idxFileName string) string {
	return strings.TrimRight(idxFileName, ".idx") + ".data"
}

/*
 when the length of a record is more than the DATA_BLOCK_SIZE, will across multiple blocks, and padded with '\0' at the end to filling-in the entire block
 this func is used to find how many '\0' was padded
 @param buffLen : length of the record buff, not contain the length of startId, endId fields
 @return paddedSize: size of the padded parts
  */
func getPaddedSize(buffLen uint64) (uint64) {
	tailSize := (DATA_HEAD_SIZE + buffLen) % DATA_BLOCK_SIZE
	if tailSize == 0 {
		return uint64(0)
	} else {
		return DATA_BLOCK_SIZE - tailSize
	}
}

func newIdxMgrSorter(disk *diskIo) *idxMgrSorter {
	sorter := &idxMgrSorter{
		disk: disk,
		items: make([]item, 0, len(disk.idxMgr.mapIndex)),
	}

	for name, info := range disk.idxMgr.mapIndex {
		sorter.items = append(sorter.items, item{name, info})
	}

	return sorter
}

func (this *idxMgrSorter) Len() int {
	return len(this.items)
}

func (this *idxMgrSorter) Less(i, j int) bool {
	startIdi, _ := this.disk.getStartIdByFileName(this.items[i].fileName)
	startIdj, _ := this.disk.getStartIdByFileName(this.items[j].fileName)
	return startIdi < startIdj
}

func (this *idxMgrSorter) Swap(i, j int) {
	this.items[i], this.items[j] = this.items[j], this.items[i]
}
