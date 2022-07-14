package key_value_storage

type KeyValueStorage interface {
	Put(key string, value string)
	Get(key string) (string, error)
}
