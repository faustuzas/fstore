package store

import "log"

type Store interface {
	Get(key string) (string, bool, error)
	Put(key string, value string) error
	Delete(key string) error

	Done() error
}

var globalStore Store
func init() {
	store, err := NewFileLogStore()
	if err != nil {
		log.Fatal(err)
	}
	globalStore = store
}

func Get(key string) (string, bool, error) {
	return globalStore.Get(key)	
}

func Put(key string, value string) error {
	return globalStore.Put(key, value)
}

func Delete(key string) error {
	return globalStore.Delete(key)
}

func Done() error {
	return globalStore.Done()
}
