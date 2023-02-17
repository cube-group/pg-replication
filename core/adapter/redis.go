package adapter

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
)

func NewRedisLsnAdapter(options *redis.Options) *RedisLsnAdapter {
	conn := redis.NewClient(options)
	if err := conn.Ping().Err(); err != nil {
		log.Fatalf("NewLsnRedisAdapter %v", err)
	}
	return &RedisLsnAdapter{conn: conn}
}

type RedisLsnAdapter struct {
	conn *redis.Client
	LsnAdapter
}

func (t *RedisLsnAdapter) key(key string) string {
	return fmt.Sprintf("pgx:replication:%s", key)
}

func (t *RedisLsnAdapter) Close() error {
	return t.conn.Close()
}

func (t *RedisLsnAdapter) Set(key string, value uint64) error {
	return t.conn.Set(t.key(key), value, 0).Err()
}

func (t *RedisLsnAdapter) Get(key string) (uint64, error) {
	return t.conn.Get(t.key(key)).Uint64()
}
