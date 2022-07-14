package main

import (
	"github.com/coutvv/kabanchik-db/internal/db/key_value_storage"
	"os"
	"time"
)

const dbFilename = "simple.db"

func main() {
	println("project has started")
	var repository = key_value_storage.CreateKeyValueLogStorage(dbFilename)

	repository.Put("123", "merely")
	var record, _ = repository.Get("123")
	println(record)
	time.Sleep(3_000_000_000)
	os.Remove(dbFilename)
}
