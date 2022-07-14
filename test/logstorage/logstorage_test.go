package logstorage

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
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

func TestLogStorageReadBench(t *testing.T) {
	var testStorage = db.CreateKeyValueLogStorage(testDbFilename)
	var key = "keyval"
	var valuePrefix = "value #"
	const max = 50000
	for i := 0; i <= max; i++ {
		testStorage.Put(key, valuePrefix+fmt.Sprint(i))
	}

	var start = time.Now().UnixNano()
	var result, _ = testStorage.Get(key)
	var elapsed = time.Now().UnixNano() - start

	if elapsed > 500_000 {
		t.Error("Too long searching operation for " + fmt.Sprint(max) + " operations")
	}
	if result != valuePrefix+fmt.Sprint(max) {
		t.Error("Incorrect value in db", result)
	}
	log.Println("Elapsed : " + fmt.Sprint(elapsed) + " ms")
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
