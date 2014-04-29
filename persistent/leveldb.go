package persistent

import (
	"os"
	"path/filepath"

	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"github.com/go-distributed/epaxos"
)

type LevelDB struct {
	fpath string
	ldb   *leveldb.DB
	wsync *db.WriteOptions
}

func NewLevelDB(path string) (*LevelDB, error) {
	fpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	ldb, err := leveldb.Open(fpath, nil) // TODO: tune the option
	if err != nil {
		return nil, err
	}

	ret := &LevelDB{
		fpath: fpath,
		ldb:   ldb,
		wsync: &db.WriteOptions{Sync: true},
	}
	return ret, nil
}

func (l *LevelDB) Put(key string, value []byte) error {
	return l.ldb.Set([]byte(key), value, l.wsync)
}

func (l *LevelDB) Get(key string) ([]byte, error) {
	return l.ldb.Get([]byte(key), nil)
}

func (l *LevelDB) Delete(key string) error {
	return l.ldb.Delete([]byte(key), l.wsync)
}

func (l *LevelDB) BatchPut(kvs ...*epaxos.KVpair) error {
	b := new(leveldb.Batch)
	for i := range kvs {
		b.Set([]byte(kvs[i].Key), kvs[i].Value)
	}
	return l.ldb.Apply(*b, l.wsync)
}

func (l *LevelDB) Close() error {
	return l.ldb.Close()
}

func (l *LevelDB) Drop() error {
	return os.RemoveAll(l.fpath)
}
