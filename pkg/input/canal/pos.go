package canal

import (
	"encoding/json"
	"fmt"
	"go-data-flow/pkg/logs"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
)

type PosSaver interface {
	Save(mysql.Position) error
	Get() (mysql.Position, error)
}

type RedisPosSaver struct {
	rdb        *redis.Client
	keyPrefix  string
	id         string
	lastSaveAt time.Time
	dip        int
}

func NewRedisPosSaver(rdb *redis.Client, keyPrefix string, id string) *RedisPosSaver {
	return &RedisPosSaver{
		rdb:       rdb,
		keyPrefix: keyPrefix,
		id:        id,
	}
}

func (s *RedisPosSaver) key() string {
	return fmt.Sprintf("%s:binlog_position:%s", s.keyPrefix, s.id)
}

func (s *RedisPosSaver) Save(pos mysql.Position) error {
	const saveInterval = 5 // seconds
	const maxDip = 10

	now := time.Now()
	s.dip++

	if now.Sub(s.lastSaveAt) > time.Second*time.Duration(saveInterval) || s.dip > maxDip {
		key := s.key()
		raw, err := json.Marshal(pos)
		if err != nil {
			return fmt.Errorf("failed to marshal binlog position: %w", err)
		}

		log.Info().
			Str(logs.PosSaver, key).
			Str("binlog_name", pos.Name).
			Uint32("binlog_pos", pos.Pos).
			Msg("Saving binlog position")

		if err := s.rdb.Set(key, raw, 0).Err(); err != nil {
			return fmt.Errorf("failed to save binlog position in Redis: %w", err)
		}

		s.lastSaveAt = now
		s.dip = 0
	}

	return nil
}

func (s *RedisPosSaver) Get() (mysql.Position, error) {
	key := s.key()
	result, err := s.rdb.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			log.Warn().Str(logs.PosSaver, key).Msg("No binlog position found, starting from scratch")
			return mysql.Position{}, nil
		}
		return mysql.Position{}, fmt.Errorf("failed to get binlog position from Redis: %w", err)
	}

	var pos mysql.Position
	if err := json.Unmarshal([]byte(result), &pos); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to unmarshal binlog position: %w", err)
	}

	log.Info().
		Str(logs.PosSaver, key).
		Str("binlog_name", pos.Name).
		Uint32("binlog_pos", pos.Pos).
		Msg("Successfully retrieved binlog position")

	return pos, nil
}
