package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/redis/go-redis/v9"
)

var _ datastore.Datastore = (*Datastore)(nil)
var _ datastore.Batching = (*Datastore)(nil)

type Datastore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewDataStore(options *redis.Options) *Datastore {
	return &Datastore{
		client: redis.NewClient(options),
		ttl:    3600,
	}
}

func (ds *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	err := ds.client.Set(ctx, key.String(), value, ds.ttl)
	if err != nil {
		return err.Err()
	}

	return nil
}

func (ds *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	ds.client.Sync(ctx)
	return nil
}

func (ds *Datastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	value, err := ds.client.Get(ctx, key.String()).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, datastore.ErrNotFound
		}
		return nil, err
	}

	return value, nil
}

func (ds *Datastore) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	size, err := ds.client.StrLen(ctx, key.String()).Result()
	if err != nil {
		return 0, err
	}
	return int(size), nil
}

func (ds *Datastore) Has(ctx context.Context, key datastore.Key) (bool, error) {
	res, err := ds.client.Exists(ctx, key.String()).Result()
	if err != nil {
		return false, err
	}

	if res != 0 {
		fmt.Println(res)
		return true, nil
	} else {
		return false, nil
	}
}

func (ds *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	_, err := ds.client.Del(ctx, key.String()).Result()
	if err != nil {
		return err
	}
	return nil
}

func (ds *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	keys := make([]query.Entry, 0)

	iter := ds.client.Scan(ctx, 0, "", 10).Iterator()
	for iter.Next(ctx) {
		e := query.Entry{Key: iter.Val(), Size: len(iter.Val())}
		if !q.KeysOnly {
			val, err := ds.client.Get(ctx, iter.Val()).Bytes()
			if err != nil {
				return nil, err
			}
			e.Value = val
		}
		keys = append(keys, e)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	r := query.ResultsWithEntries(q, keys)
	r = query.NaiveQueryApply(q, r)

	return r, nil
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}

func (ds *Datastore) Close() error {
	err := ds.client.Close()
	if err != nil {
		return err
	}

	return nil
}
