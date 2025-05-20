package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var hosts = []string{}
var delKey = "keys:*"
var isClear = false
var withValue = false

func main() {

	flag.Parse()

	fmt.Println("Host:", hosts)
	fmt.Println("Key:", delKey)
	fmt.Println("Clear:", isClear)
	fmt.Println("WithValue:", withValue)

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: hosts,
	})
	rdb.Ping(ctx)

	err := rdb.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
		iter := rdb.Scan(ctx, 0, delKey, 500).Iterator()

		for iter.Next(ctx) {
			key := iter.Val()
			keyType, err := rdb.Type(ctx, key).Result()
			if err != nil {
				fmt.Printf("key: %s, type check error: %v\n", key, err)
				continue
			}

			var val interface{}

			if withValue {
				switch keyType {
				case "string":
					val, err = rdb.Get(ctx, key).Result()
				case "hash":
					val, err = rdb.HGetAll(ctx, key).Result()
				case "list":
					val, err = rdb.LRange(ctx, key, 0, -1).Result()
				case "set":
					val, err = rdb.SMembers(ctx, key).Result()
				case "zset":
					val, err = rdb.ZRange(ctx, key, 0, -1).Result()
				default:
					fmt.Printf("key: %s, unsupported type: %s\n", key, keyType)
					continue
				}

				if err != nil {
					fmt.Printf("key: %s, get value error: %v\n", key, err)
					continue
				}
				fmt.Printf("key: %s, type: %s, value: %v\n", key, keyType, val)
			} else {
				fmt.Println(key)
			}

			if isClear {
				err := rdb.Del(ctx, key).Err()
				if err != nil {
					panic(err)
				}
			}
		}

		return iter.Err()
	})
	if err != nil {
		panic(err)
	}
}

func init() {
	hostString := "127.0.0.1:6379;127.0.0.1:6380;127.0.0.1:6381;127.0.0.1:6382"
	flag.BoolVar(&isClear, "clear", false, "clear")
	flag.StringVar(&delKey, "key", delKey, "key parttern")
	flag.StringVar(&hostString, "hosts", hostString, "host list")
	flag.BoolVar(&withValue, "withValue", false, "with value")
	flag.Parse()	

	hosts = strings.Split(hostString, ";")
}
