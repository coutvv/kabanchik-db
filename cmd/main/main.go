package main

import (
	"github.com/coutvv/kabanchik-db/internal/db/key_value_storage"
)

func main() {
	println("project has started")
	var repository = key_value_storage.CreateKeyValueLogStorage("simple.db")
	repository.Put("123", "merely")
	var record, _ = repository.Get("123")
	println(record)
}
