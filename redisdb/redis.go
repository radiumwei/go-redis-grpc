package redisdb

import (
	"context"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type RedisClient interface {
	Command(context.Context, string) (string, error)
	Subscribe(...string) (string, string)
	Publish(context.Context, string, string) (int64, error)
	Set(context.Context, string, string, time.Duration) (string, error)
	Get(context.Context, string) (string, error)
	Keys(context.Context, string) ([]string, error)
	Delete(context.Context, string) (int64, error)
	Lpush(context.Context, string, string) (int64, error)
	Rpush(context.Context, string, string) (int64, error)
}

type redisClient struct {
	client *redis.Client
}

func CreateRedisClient(ctx context.Context, dbaddress string) (RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     dbaddress,
		Password: "", // No password
		DB:       0,  // Default DB
	})
	_, err := client.Ping().Result()
	if err != nil {
		return nil, &CreateDatabaseError{}
	}
	return &redisClient{client: client}, nil
}

func (r *redisClient) Command(ctx context.Context, cmd string) (string, error) {
	switch cmd {
	case "PING":
		{
			value, err := r.client.Ping().Result()
			if err != nil {
				return valueStringError("ping", "", err)
			}
			return value, nil
		}
	case "FLUSHALL":
		{
			value, err := r.client.FlushAll().Result()
			if err != nil {
				return valueStringError("flushall", "", err)
			}
			return value, nil
		}
	case "TIME":
		{
			value, err := r.client.Time().Result()
			if err != nil {
				return valueStringError("time", "", err)
			}
			return strconv.FormatInt(value.Unix(), 10), nil
		}
	}

	return "Unknown Command", &OperationError{cmd}

}

func (r *redisClient) Subscribe(channels ...string) (string, string) {
	pubsub := r.client.Subscribe(channels...)
	defer pubsub.Close()
	for msg := range pubsub.Channel() {
		return msg.Channel, msg.Payload
	}

	return "", ""
}

func (r *redisClient) Publish(ctx context.Context, channel string, message string) (int64, error) {
	value, err := r.client.Publish(channel, message).Result()
	if err != nil {
		return valueInt64Error("publish", 0, err)
	}
	return value, nil
}

func (r *redisClient) Set(ctx context.Context, key string, value string, ttl time.Duration) (string, error) {
	_, err := r.client.Set(key, value, ttl).Result()
	if err != nil {
		return valueStringError("set", "", err)
	}
	return key, nil
}

func (r *redisClient) Get(ctx context.Context, key string) (string, error) {
	value, err := r.client.Get(key).Result()
	if err != nil {
		return valueStringError("get", "", err)
	}
	return value, nil

}

func (r *redisClient) Delete(ctx context.Context, key string) (int64, error) {
	value, err := r.client.Del(key).Result()
	if err != nil {
		return valueInt64Error("delete", 0, err)
	}
	return value, nil
}

func (r *redisClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	values, err := r.client.Keys(pattern).Result()
	if err != nil {
		return valueStringArrayError("keys", []string{}, err)
	}
	return values, nil
}

func (r *redisClient) Lpush(ctx context.Context, key string, element string) (int64, error) {
	value, err := r.client.LPush(key, element).Result()
	if err != nil {
		return valueInt64Error("lpush", 0, err)
	}
	return value, nil
}

func (r *redisClient) Rpush(ctx context.Context, key string, element string) (int64, error) {
	value, err := r.client.RPush(key, element).Result()
	if err != nil {
		return valueInt64Error("rpush", 0, err)
	}
	return value, nil
}

func valueInt64Error(operation string, value int64, err error) (int64, error) {
	if err == redis.Nil {
		return value, &OperationError{operation}
	}
	return value, &DownError{}
}

func valueStringError(operation string, value string, err error) (string, error) {
	if err == redis.Nil {
		return value, &OperationError{operation}
	}
	return value, &DownError{}
}

func valueStringArrayError(operation string, values []string, err error) ([]string, error) {
	if err == redis.Nil {
		return values, &OperationError{operation}
	}
	return values, &DownError{}
}
