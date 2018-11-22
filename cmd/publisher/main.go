package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/antekresic/grs/check"
	"github.com/antekresic/grs/server"
	"github.com/antekresic/grs/storage"
	"github.com/go-playground/validator"
	"github.com/go-redis/redis"
)

var (
	port      = flag.Int("port", 80, "HTTP port for the service")
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

	r := storage.RedisRepository{
		Client: redisClient,
	}

	v := check.Entry{
		Validator: validator.New(),
	}

	s := server.HTTP{
		Repo:      &r,
		Validator: v,
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), s))
}
