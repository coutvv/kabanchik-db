package logstorage

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)
import db "github.com/coutvv/kabanchik-db/internal/db/key_value_storage"

var testDbFilename = "test.db"

func TestLogStorageWriteValue(t *testing.T) {
	os.Remove(testDbFilename)
	var testStorage = db.CreateKeyValueLogStorage(testDbFilename)
	testStorage.Put("id", "value")

	var bytesOfFile, err = ioutil.ReadFile(testDbFilename)
	if err != nil {
		log.Fatal("can't read from db file", err)
	}
	var content = string(bytesOfFile)

	resultString := "id,value\n"

	if content != resultString {
		t.Error("incorrect written value in bytesOfFile")
	}
}

func TestLogStorageReadLastValue(t *testing.T) {
	var testStorage = db.CreateKeyValueLogStorage(testDbFilename)
	testStorage.Put("id2", "value")
	testStorage.Put("id2", "value2")
	var result, err = testStorage.Get("id2")

	if result != "value2" || err != nil {
		t.Error("incorrect reading or writing value")
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	terminate()
	os.Exit(code)
}

func terminate() {
	err := os.Remove(testDbFilename)
	if err != nil {
		log.Fatal("can't cleanup test db", err)
	}
}
