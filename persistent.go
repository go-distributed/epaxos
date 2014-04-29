package epaxos

type KVpair struct {
	Key   string
	Value []byte
}

type Persistent interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	BatchPut(kvs []*KVpair) error
}
