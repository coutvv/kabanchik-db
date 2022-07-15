package main

import (
	"github.com/coutvv/kabanchik-db/internal/db/key_value_storage"
	"github.com/coutvv/kabanchik-db/internal/db/util"
	"os"
)

const dbFilename = "simple.db"

func main() {
	var repository = key_value_storage.CreateKeyValueLogStorageIndexed(dbFilename)
	defer os.Remove(dbFilename)

	repository.Put("123", "merely")
	var record, _ = repository.Get("123")

	println(record)

	util.WaitForIt(5)
}
