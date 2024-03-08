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

func main() {

	flag.Parse()

	fmt.Println("Host:", hosts)
	fmt.Println("Key:", delKey)
	fmt.Println("Clear:", isClear)

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: hosts,
	})
	rdb.Ping(ctx)

	err := rdb.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
		iter := rdb.Scan(ctx, 0, delKey, 500).Iterator()

		for iter.Next(ctx) {
			fmt.Println(iter.Val())

			if isClear {
				err := rdb.Del(ctx, iter.Val()).Err()
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

	hosts = strings.Split(hostString, ";")
}
