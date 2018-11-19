package main

import (
	"log"

	"github.com/antekresic/grs/consumer"
	"github.com/antekresic/grs/storage"
	"github.com/go-redis/redis"
	"github.com/satori/go.uuid"
)

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	_, err := redisClient.Ping().Result()

	if err != nil {
		log.Fatal("Redis connection error:", err)
	}

	name := uuid.NewV4().String()

	r := storage.RedisRepository{
		Client:       redisClient,
		ConsumerName: name,
	}

	c := consumer.Printer{
		Repo: r,
		Name: name,
	}

	log.Fatal(c.StartConsuming())
}
