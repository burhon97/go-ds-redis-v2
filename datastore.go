package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	datastore "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/redis/go-redis/v9"
)

var _ datastore.Datastore = (*Datastore)(nil)
var _ datastore.Batching = (*Datastore)(nil)

var ErrClosed = errors.New("datastore closed")

type Datastore struct {
	mu     sync.RWMutex
	client *redis.Client
	ttl    time.Duration
}

func main() {
	// ctx := context.Background()

	// client := NewDataStore(&redis.Options{
	// 	Addr:     "localhost:6379",
	// 	Password: "",
	// 	DB:       0,
	// })

	// err := client.Put(ctx, "key", "Hello World!", 3600)
	// if err != nil {
	// 	fmt.Println("ERROR: ", err)
	// }

	// res, err := client.Get(ctx, "key")
	// if err != nil {
	// 	fmt.Println("ERROR 2: ", err)
	// }

	// fmt.Println(res)

	// // time.AfterFunc(3*time.Second, func() {
	// 	r, err := client.Has(ctx, "key")
	// 	if err != nil {
	// 		fmt.Println("ERROR 3: ", err)
	// 	}

	// 	fmt.Println("Has: ", r)
	// // })

	// select {}

}

func NewDataStore(options *redis.Options) *Datastore {
	client := redis.NewClient(options)

	return &Datastore{
		client: client,
		ttl:    3600,
	}
}

func (ds *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	err := ds.client.Set(ctx, key.String(), value, ds.ttl)

	return err.Err()
}

func (ds *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.client.Sync(ctx)
	return nil
}

func (ds *Datastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	value, err := ds.client.Get(ctx, key.String()).Bytes()
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (ds *Datastore) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	size, err := ds.client.StrLen(ctx, key.String()).Result()
	if err != nil {
		return 0, err
	}

	return int(size), nil
}

func (ds *Datastore) Has(ctx context.Context, key datastore.Key) (bool, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
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
	ds.mu.Lock()
	defer ds.mu.Unlock()
	_, err := ds.client.Del(ctx, key.String()).Result()
	if err != nil {
		return err
	}

	return nil
}

func (ds *Datastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	qNaive := q
	prefix := datastore.NewKey(q.Prefix).String()
	if prefix != "/" {
		prefix = prefix + "/"
	}

	i := ds.client.Scan(ctx, 0, "", 0).Iterator()
	next := i.Next(ctx)
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			qNaive.Orders = nil
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			next = i.Next(ctx)
			qNaive.Orders = nil
		default:
		}
	}
	r := dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			ds.mu.RLock()
			defer ds.mu.RUnlock()
			if !next {
				return dsq.Result{}, false
			}
			k := string(i.Val())
			e := dsq.Entry{Key: k, Size: len(i.Val())}

			if !q.KeysOnly {
				buf := make([]byte, len(i.Val()))
				copy(buf, i.Val())
				e.Value = buf
			}
			return dsq.Result{Entry: e}, true
		},
		Close: func() error {
			ds.mu.RLock()
			defer ds.mu.RUnlock()
			return nil
		},
	})
	return dsq.NaiveQueryApply(qNaive, r), nil
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return nil, datastore.ErrBatchUnsupported
}

func (ds *Datastore) Close() error {
	err := ds.client.Close()
	if err != nil {
		return err
	}

	return nil
}
