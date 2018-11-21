package main

import (
	"log"

	"github.com/antekresic/grs/consumer"
	"github.com/antekresic/grs/storage"
	"github.com/antekresic/grs/streamer"
	"github.com/go-redis/redis"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	_, err := redisClient.Ping().Result()

	if err != nil {
		log.Fatal("Redis connection error:", err)
	}

	s := &streamer.RedisStreamer{
		Repo: &storage.RedisRepository{
			Client: redisClient,
		},
	}

	c := consumer.Printer{
		Streamer: s,
	}

	log.Fatal(c.StartConsuming())
}
