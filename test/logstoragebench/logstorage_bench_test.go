package logstoragebench

import (
	"fmt"
	"github.com/coutvv/kabanchik-db/internal/db/util"
	"log"
	"os"
	"testing"
)
import db "github.com/coutvv/kabanchik-db/internal/db/key_value_storage"

const testDbFilename = "bench.db"
const max = int(5000)
const key = "keyval"
const numOfReads = 10000

var value = "value #" + fmt.Sprint(max)

func TestLogStorageIndexedRead(t *testing.T) {
	defer util.Timer("hash indexed storage read operation")()

	var hashIndexedStorage = db.CreateKeyValueLogStorageIndexed(testDbFilename)
	for i := 0; i < numOfReads; i++ {
		var result, _ = hashIndexedStorage.Get(key)
		if result != value {
			t.Error("Incorrect value in db", result)
		}
	}
}

func TestLogStorageRead(t *testing.T) {
	defer util.Timer("hash indexed storage read operation")()
	var logStorage = db.CreateKeyValueLogStorage(testDbFilename)

	for i := 0; i < numOfReads; i++ {
		var result, _ = logStorage.Get(key)
		if result != value {
			t.Error("Incorrect value in db", result)
		}
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	terminate()
	os.Exit(code)
}

func setup() {
	var logStorage = db.CreateKeyValueLogStorage(testDbFilename)
	for i := 0; i <= max; i++ {
		logStorage.Put(key, "blabla"+fmt.Sprint(i))
	}
	logStorage.Put(key, value)
}

func terminate() {
	err := os.Remove(testDbFilename)
	if err != nil {
		log.Fatal("can't cleanup test db", err)
	}
}
