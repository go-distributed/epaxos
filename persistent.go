package epaxos

import (
	"errors"
)

type KVpair struct {
	Key   string
	Value []byte
}

var ErrorNotFound = errors.New("persistent: not found")

type Persistent interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	BatchPut(kvs []*KVpair) error
}
