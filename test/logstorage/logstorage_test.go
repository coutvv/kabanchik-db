package logstorage

import (
	"fmt"
	db "github.com/coutvv/kabanchik-db/internal/db/key_value_storage"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

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

	defer timer("read operation in small db")()
	var testStorage = db.CreateKeyValueLogStorage(testDbFilename)
	testStorage.Put("id1", "value")
	testStorage.Put("id1", "value2")
	var result, err = testStorage.Get("id1")

	if result != "value2" || err != nil {
		t.Error("incorrect reading or writing value: " + result)
	}
}

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	afterTest()
	os.Exit(code)
}

func afterTest() {
	err := os.Remove(testDbFilename)
	if err != nil {
		log.Fatal("can't cleanup test db", err)
	}
}
