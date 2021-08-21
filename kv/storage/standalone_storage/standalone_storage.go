package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	//According to engines.go
	st_engine engine_util.Engines
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	//According to CreateDB() in engines.go
	Kv := engine_util.CreateDB(conf.DBPath, conf.Raft)

	return &StandAloneStorage{
		st_engine: *engine_util.NewEngines(Kv, Kv, conf.DBPath, conf.DBPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	// No need to start? or ... opened in NewStandAloneStorage
	// with CreateDB()
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	//Close means to Stop...?
	// return s.st_engine.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.st_engine.Kv.NewTransaction(false) // false for readonly
	st_reader := &StandAloneReader{
		txn: txn,
	}
	if st_reader != nil {
		return st_reader, nil
	} else {
		return nil, nil
	}
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	wb := new(engine_util.WriteBatch)

	for _, val := range batch {
		switch val.Data.(type) {
		case storage.Put:
			{
				wb.SetCF(val.Cf(), val.Key(), val.Value())
			}
		case storage.Delete:
			{
				wb.DeleteCF(val.Cf(), val.Key())
			}
		}
	}

	wb.MustWriteToDB(s.st_engine.Kv)

	return nil //already handle error in must...
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {

	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {

	return engine_util.NewCFIterator(cf, r.txn)

}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
	// r.Close() infinite call
}
