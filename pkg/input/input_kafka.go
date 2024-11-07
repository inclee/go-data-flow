package input

import (
	"context"
	"encoding/json"
	"go-data-flow/pkg/logs"
	"go-data-flow/pkg/stream"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	// gzip
	_ "github.com/segmentio/kafka-go/gzip"
	_ "github.com/segmentio/kafka-go/lz4"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	_ "github.com/segmentio/kafka-go/snappy"
)

type KafkaInputConfig struct {
	Topic    string   `yaml:"topic"`
	GroupID  string   `yaml:"group"`
	Brokers  []string `yaml:"brokers"`
	Latest   bool     `yaml:"latest"`
	User     string   `yaml:"user"`
	PassWord string   `yaml:"password"`
	SASL     string   `yaml:"sasl"`
}

type kafkaInput struct {
	KafkaInputConfig
	BaseInput
	reader *kafka.Reader `yaml:"-"`
	stream *stream.Scream
	stop   bool
	logger zerolog.Logger
}

func NewKafkaInput(base BaseInput, kafkaCfg *KafkaInputConfig) (Input, error) {
	plugin := &kafkaInput{
		BaseInput:        base,
		KafkaInputConfig: *kafkaCfg,
	}

	startOffset := kafka.FirstOffset
	if plugin.Latest {
		startOffset = kafka.LastOffset
	}
	cfg := kafka.ReaderConfig{Brokers: plugin.Brokers, GroupID: plugin.GroupID, Topic: plugin.Topic, StartOffset: startOffset}
	if plugin.User != "" {
		var mechanism sasl.Mechanism
		var err error
		switch plugin.SASL {
		case "plain":
			mechanism = plain.Mechanism{Username: plugin.User, Password: plugin.PassWord}
		case "sha512":
			mechanism, err = scram.Mechanism(scram.SHA512, plugin.User, plugin.PassWord)
		case "sha256":
			mechanism, err = scram.Mechanism(scram.SHA256, plugin.User, plugin.PassWord)
		}
		if err != nil {
			return nil, err
		}
		cfg.Dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
		}
	}
	plugin.logger = log.With().Any(logs.Input, "Kafka").Logger()
	plugin.reader = kafka.NewReader(cfg)
	plugin.stream = stream.NewSteam()
	return plugin, nil
}

func (k *kafkaInput) Flow(ctx context.Context) *stream.Scream {
	go func() {
		<-k.Context().Done()
		k.logger.Info().Msg("stopping!")
		k.stop = true
	}()
	go func() {
		defer func() {
			k.logger.Info().Msg("stoped!")
		}()
		for !k.stop {
			msg, err := k.reader.ReadMessage(ctx)
			k.logger.Info().Str("topic", msg.Topic).Any("partition", msg.Partition).Any("offset", msg.Offset).Err(err).Msg("read")
			if err == context.Canceled {
				return
			} else if err != nil {
				k.stream.Err <- err
				return
			}
			event := stream.Event{Context: ctx}
			if err = json.Unmarshal(msg.Value, &event); err != nil {
				k.logger.Info().Str("topic", msg.Topic).Any("partition", msg.Partition).Any("offset", msg.Offset).Any("raw data", string(msg.Value)).Err(err).Msg("unmarshal failed")
				k.stream.Err <- err
			} else {
				k.stream.In <- event
				result := <-k.stream.Out
				if result.Error != nil {
					k.logger.Info().Str("topic", msg.Topic).Any("partition", msg.Partition).Any("offset", msg.Offset).Any("raw data", string(msg.Value)).Err(err).Msg("process error")
					k.stream.Err <- result.Error
				} else {
					k.reader.CommitMessages(ctx, msg)
				}
			}
		}
	}()
	return k.stream
}
