package key_value_storage

import (
	"bufio"
	"errors"
	"log"
	"os"
	"strings"
)

type LogStorage struct {
	filename    string
	dbHashIndex map[string]int64 // key to offset in file
}

func (ls *LogStorage) Put(key string, value string) {
	f, err := os.OpenFile(ls.filename, os.O_APPEND|os.O_WRONLY|os.O_SYNC, os.ModeAppend)
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

func (ls *LogStorage) Get(key string) (string, error) {
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
	defer file.Close()
	reader := bufio.NewReader(file)

	reader.Discard(int(countBytesToSkip))
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

// deprecated
func (ls *LogStorage) getByBruteForce(key string) (string, error) {
	file, err := os.Open(ls.filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var recordPrefix = key + ","

	scanner := bufio.NewScanner(file)
	var result string
	for scanner.Scan() {
		var record = scanner.Text()
		if strings.HasPrefix(record, recordPrefix) {
			result = record[len(recordPrefix):]
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	if result != "" {
		return result, nil
	}
	return "", errors.New("no value")
}

func (ls *LogStorage) createHashIndex() {
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

func CreateKeyValueLogStorage(filename string) KeyValueStorage {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		file, error := os.Create(filename)
		if error != nil {
			log.Fatal("can't create db", error)
		}
		file.Close()
	}
	var result = &LogStorage{
		filename:    filename,
		dbHashIndex: map[string]int64{},
	}
	result.createHashIndex()
	return result
}
