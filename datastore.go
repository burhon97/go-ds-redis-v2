package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type DataStore struct {
	client *redis.Client
}



// func main (){
// ctx := context.Background()

// 	client := NewDataStore(&redis.Options{
// 		Addr: "localhost:6379",
// 		Password: "",
// 		DB: 0,
// 	})

	

// 	err := Set(client, ctx, "key", "Hello World!", 3600)
// 	if err != nil {
// 		fmt.Println("ERROR: ", err)
// 	}

// 	res, err := Get(client, ctx, "key")
// 	if err != nil {
// 		fmt.Println("ERROR 2: ", err)
// 	}

// 	fmt.Println(res)

// }


// type redisOpt interface {
// 	Set(ctx context.Context, key string, value interface{}, expiration time.Duration)(error)
// 	Get(ctx context.Context, key string)(string, error)
// }


func NewDataStore(options *redis.Options)(*redis.Client){
	rdb := redis.NewClient(options)

	return rdb
}

func Set(client *redis.Client, ctx context.Context, key string, value interface{}, expiration time.Duration)(error){
	err := client.Set(ctx, key, value, expiration)

	return err.Err()
}

func Get(client *redis.Client, ctx context.Context, key string)(string, error){
	value, err := client.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}

	return value, nil
}

func Close(client *redis.Client)(error){
	err := client.Close()

	return err
}