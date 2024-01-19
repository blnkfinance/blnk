package redis_db

import (
	"errors"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	addresses []string
	client    redis.UniversalClient
}

func NewRedisClient(addresses []string) (*Redis, error) {
	if len(addresses) == 0 {
		return nil, errors.New("redis addresses list cannot be empty")
	}

	var client redis.UniversalClient

	if len(addresses) == 1 {
		opts, err := redis.ParseURL(addresses[0])
		if err != nil {
			return nil, err
		}

		client = redis.NewClient(opts)
	} else {
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: addresses,
		})
	}

	return &Redis{addresses: addresses, client: client}, nil
}

func (r *Redis) Client() redis.UniversalClient {
	return r.client
}

func (r *Redis) MakeRedisClient() interface{} {
	return r.client
}
