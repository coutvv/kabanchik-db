package key_value_storage

import (
	"bufio"
	"errors"
	"log"
	"os"
	"strings"
)

type LogStorage struct {
	filename string
}

func (ls *LogStorage) Put(key string, value string) {
	f, err := os.OpenFile(ls.filename, os.O_APPEND|os.O_WRONLY|os.O_SYNC, os.ModeAppend)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	record := key + "," + value + "\n"
	if _, err = f.WriteString(record); err != nil {
		log.Fatal(err)
	}
}

func (ls *LogStorage) Get(key string) (string, error) {
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

func CreateKeyValueLogStorage(filename string) KeyValueStorage {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		file, error := os.Create(filename)
		if error != nil {
			log.Fatal("can't create db", error)
		}
		file.Close()
	}
	return &LogStorage{
		filename: filename,
	}
}
