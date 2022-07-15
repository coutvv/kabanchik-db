package key_value_storage

import (
	"bufio"
	"errors"
	"log"
	"os"
	"strings"
)

/**
I'm lazy right now, but this storage needs some improvements...
So for the future implement this issues:
- TODO: Cut whole db file to segments
- TODO: Add compaction for segmentation files (for getting rid of obsolete data)
- TODO: Replace txt files with binary format
- TODO: Add feature "Removing record"
- TODO: Save hashIndex to the disk periodically and restore it with restarting
- TODO: Make Read operation concurrent
*/
type LogStorageIndexed struct {
	filename    string
	dbHashIndex map[string]int64 // key to offset in file
}

func (ls *LogStorageIndexed) Put(key string, value string) {
	f, err := os.OpenFile(ls.filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	info, err := f.Stat()
	offset := info.Size()
	ls.dbHashIndex[key] = offset

	record := key + "," + value + "\n"
	if _, err = f.WriteString(record); err != nil {
		log.Fatal(err)
	}

}

func (ls *LogStorageIndexed) Get(key string) (string, error) {
	var countBytesToSkip, has = ls.dbHashIndex[key]
	if !has {
		log.Println("not found the value for key" + key)
		return "", errors.New("no record in hash index and in the file")
	}
	file, err := os.Open(ls.filename)
	defer file.Close()
	if err != nil {
		log.Println(err)
		return "", errors.New("can't open db file")
	}
	_, err = file.Seek(countBytesToSkip, 0) // Set the current position for the fd
	if err != nil {                         // error handler
		log.Fatal(err)
	}
	reader := bufio.NewReader(file)

	lineBytes, _, err := reader.ReadLine()

	if err != nil {
		log.Println("can't find value for key = "+key, err)
		return "", err
	}
	var record = string(lineBytes)
	valueStartIndex := len(key) + 1 // +1 because of delimiter
	result := record[valueStartIndex:]
	return result, nil
}

func (ls *LogStorageIndexed) createHashIndex() {
	file, err := os.Open(ls.filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var pointer int64 = 0
	for scanner.Scan() {
		var record = scanner.Text()
		key := strings.Split(record, ",")[0]
		ls.dbHashIndex[key] = pointer
		pointer += int64(len(record)) + 1 // +1 is \n symbol
	}
}

func CreateKeyValueLogStorageIndexed(filename string) KeyValueStorage {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		file, error := os.Create(filename)
		if error != nil {
			log.Fatal("can't create db", error)
		}
		file.Close()
	}
	var result = &LogStorageIndexed{
		filename:    filename,
		dbHashIndex: map[string]int64{},
	}
	result.createHashIndex()
	return result
}
