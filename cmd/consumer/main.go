package main

import (
	"log"

	"github.com/antekresic/grs/consumer"
	"github.com/antekresic/grs/storage"
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

	r := storage.RedisRepository{
		Client: redisClient,
	}

	c := consumer.Printer{
		Repo: &r,
	}

	log.Fatal(c.StartConsuming())
}
