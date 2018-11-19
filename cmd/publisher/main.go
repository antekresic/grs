package main

import (
	"log"
	"net/http"

	"github.com/antekresic/grs/check"
	"github.com/antekresic/grs/server"
	"github.com/antekresic/grs/storage"
	"github.com/go-playground/validator"
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

	v := check.Entry{
		Validator: validator.New(),
	}

	s := server.HTTP{
		Repo:      r,
		Validator: v,
	}

	log.Fatal(http.ListenAndServe(":8808", s))
}
