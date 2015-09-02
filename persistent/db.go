package persistent

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/go-distributed/epaxos"
)

var BUCKET_NAME []byte = []byte{'e', 'p', 'a', 'x' , 'o', 's'}
type DB interface {
	GetPath()	string
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	BatchPut(kvs []*epaxos.KVpair) error
	Close() error
	Drop() error
}
type BoltDB struct {
	db		*bolt.DB
	fpath	string
}
/*
type LevelDB struct {
	fpath string
	ldb   *leveldb.DB
	wsync *db.WriteOptions
}
*/

func NewBoltDB(path string, restore bool) (*BoltDB, error) {
	fpath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	if !restore {
		err = os.Remove(fpath)
		if err != nil && os.IsExist(err) {
				return nil, err
		}
	}

	db, err := bolt.Open(fpath, 0600, nil) // TODO: tune the option
	if err != nil {
		return nil, err
	}

	err2 := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(BUCKET_NAME)
		return nil
	})
	if err2 != nil {
		db.Close()
		return nil, err2
	}

	ret := &BoltDB{
		db:	db,
		fpath: fpath,
	}
	return ret, nil
}

func (d *BoltDB) Put(key string, value []byte) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		tx.Bucket(BUCKET_NAME).Put([]byte(key), value)
		return nil
	})
	return err
}
func (d *BoltDB) Get(key string) ([]byte, error) {
	var ret []byte
	found := false
	err := d.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(BUCKET_NAME).Get([]byte(key))
		if value != nil {
			found = true
			ret = make([]byte, len(value))
			copy(ret, value)
		}
		return nil
	})
	if found {
		return ret, err
	} else {
		return ret, epaxos.ErrorNotFound
	}
}
func (d *BoltDB) Delete(key string) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		tx.Bucket(BUCKET_NAME).Delete([]byte(key))
		return nil
	})
	return err
}

func (d *BoltDB) Close() error {
	return d.db.Close()
}

func (d *BoltDB) Drop() error {
	err := os.Remove(d.fpath)
	if os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}


func (d *BoltDB) BatchPut(kvs []*epaxos.KVpair) error {
	err := d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(BUCKET_NAME)
		for i := range kvs {
			bucket.Put([]byte(kvs[i].Key), kvs[i].Value)
		}
		return nil
	})
	return err

}

func (d *BoltDB) GetPath() string {

	return d.db.Path()
}
