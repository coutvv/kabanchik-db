package logstoragebench

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)
import db "github.com/coutvv/kabanchik-db/internal/db/key_value_storage"

const testDbFilename = "test.db"
const max = 50000
const key = "keyval"
const valuePrefix = "value #"

func TestLogStorageRead(t *testing.T) {
	defer timer("get operation")()
	var testStorage = db.CreateKeyValueLogStorage(testDbFilename)

	var start = time.Now().UnixNano()
	var result, _ = testStorage.Get(key)
	var elapsed = time.Now().UnixNano() - start

	if result != valuePrefix+fmt.Sprint(max) {
		t.Error("Incorrect value in db", result)
	}
	log.Println("Elapsed : " + fmt.Sprint(elapsed) + " ns")
}

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	terminate()
	os.Exit(code)
}

func setup() {
	var testStorage = db.CreateKeyValueLogStorage(testDbFilename)
	for i := 0; i <= max; i++ {
		testStorage.Put(key, valuePrefix+fmt.Sprint(i))
	}
}

func terminate() {
	err := os.Remove(testDbFilename)
	if err != nil {
		log.Fatal("can't cleanup test db", err)
	}
}
