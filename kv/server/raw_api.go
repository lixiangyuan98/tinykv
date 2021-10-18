package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	var err error
	resp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if len(value) == 0 {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	var err error
	resp := new(kvrpcpb.RawPutResponse)
	err = server.storage.Write(req.Context, []storage.Modify{{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	var err error
	resp := new(kvrpcpb.RawDeleteResponse)
	err = server.storage.Write(req.Context, []storage.Modify{{Data: storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}}})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	var err error
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	count := uint32(0)
	for ; iter.Valid() && count < req.Limit; iter.Next() {
		kv := new(kvrpcpb.KvPair)
		item := iter.Item()
		v, err := item.Value()
		if err != nil {
			return resp, err
		} else {
			kv.Key = item.Key()
			kv.Value = v
		}
		resp.Kvs = append(resp.Kvs, kv)
		count++
	}
	return resp, nil
}
