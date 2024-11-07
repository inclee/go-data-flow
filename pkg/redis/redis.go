package redis

import (
	"fmt"

	"github.com/go-redis/redis"
)

var Ins *redis.Client

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

func Init(cfg RedisConfig) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,     // Redis 服务器地址
		Password: cfg.Password, // Redis 服务器密码，如果没有密码则留空
		DB:       cfg.DB,       // Redis 数据库编号，默认为 0
	})
	_, err := rdb.Ping().Result()
	if err != nil {
		return fmt.Errorf("连接 Redis 失败:%w", err)
	}
	Ins = rdb
	return nil
}
