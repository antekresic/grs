package main

import (
	"flag"
	"log"

	"github.com/antekresic/grs/consumer"
	"github.com/antekresic/grs/storage"
	"github.com/antekresic/grs/streamer"
	"github.com/go-redis/redis"
)

var (
	redisAddr = flag.String("redis-address", ":6379", "Redis address")
)

func main() {
	flag.Parse()

	redisClient := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
	})

	_, err := redisClient.Ping().Result()

	if err != nil {
		log.Fatal("Redis connection error:", err)
	}

	s := &streamer.RedisStreamer{
		Repo: &storage.RedisRepository{
			Client: redisClient,
		},
		Clock: streamer.RealClock{},
	}

	c := consumer.Printer{
		Streamer: s,
	}

	log.Fatal(c.StartConsuming())
}
