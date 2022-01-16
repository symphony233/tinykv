package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	rd, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	}

	defer rd.Close()

	val, err := rd.GetCF(req.GetCf(), req.GetKey())

	var not_found bool = false
	var err_msg string = ""

	if err != nil {
		err_msg = err.Error()
	}
	if val == nil {
		not_found = true
	}

	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		Error:    err_msg,
		NotFound: not_found,
	}
	// rd.Close()
	return resp, nil
	// return nil, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	put_data := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	}

	server.storage.Write(req.GetContext(), put_data)

	resp := &kvrpcpb.RawPutResponse{}

	return resp, nil
	// return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	delete_data := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	}

	server.storage.Write(req.GetContext(), delete_data)

	resp := &kvrpcpb.RawDeleteResponse{}

	return resp, nil
	// return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	rd, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, nil
	}
	defer rd.Close()
	iter := rd.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())
	kvs := make([]*kvrpcpb.KvPair, 0)

	for i := 0; i < int(req.GetLimit()) && iter.Valid(); i++ {
		key := make([]byte, 0)
		value := make([]byte, 0)
		key = iter.Item().KeyCopy(key)
		value, err = iter.Item().ValueCopy(value)
		if err != nil {
			panic(fmt.Sprintf("Value Copy: %v \n", err))
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		iter.Next()
	}

	resp := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}

	return resp, nil
	// return nil, nil
}
