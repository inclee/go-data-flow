package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-data-flow/pkg/logs"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type KafkaOutputConfig struct {
	Topic           string   `yaml:"topic"`
	Key             string   `yaml:"key"`
	KeyTemplate     string   `yaml:"key_tempalte"`
	Brokers         []string `yaml:"brokers"`
	CompressionType string   `yaml:"compression.type"`
	MessageMaxCount int      `yaml:"message.max.count"`
	BulkSize        int      `yaml:"bulk_size"`
	BulkFlushSec    int      `yaml:"bulk_flush_sec"`
}

type KafkaOutput struct {
	BaseOutput
	config   *KafkaOutputConfig
	producer *kafka.Writer
	bulk     *util.Bulk[stream.Event]
	dataCh   chan []util.BulkItem[stream.Event]
	logger   zerolog.Logger
}

func NewKafkaOutput(base BaseOutput, cfg *KafkaOutputConfig) (*KafkaOutput, error) {
	p := &KafkaOutput{
		BaseOutput: base,
		config:     cfg,
	}
	if cfg.Topic == "" {
		err := errors.New("kafka output must have topic setting")
		return nil, err
	}
	if len(cfg.Brokers) == 0 {
		err := errors.New("kafka output must have bootstrap.servers setting")
		return nil, err
	}
	if cfg.BulkSize == 0 {
		cfg.BulkSize = 10
	}
	if cfg.BulkFlushSec == 0 {
		cfg.BulkFlushSec = 5
	}
	wconfig := kafka.WriterConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		Balancer: &kafka.LeastBytes{},
	}
	p.logger = log.With().Any(logs.Output, "Kafka").Logger()
	p.dataCh = make(chan []util.BulkItem[stream.Event])
	p.bulk = util.NewBulk(cfg.BulkSize, time.Duration(cfg.BulkFlushSec)*time.Second, p.dataCh)
	p.producer = kafka.NewWriter(wconfig)
	p.Run()
	return p, nil
}

func generateKey(data map[string]interface{}, template string) (string, error) {
	key := template
	for placeholder, value := range data {
		placeholderTag := fmt.Sprintf("{%s}", placeholder)
		if strings.Contains(key, placeholderTag) {
			key = strings.ReplaceAll(key, placeholderTag, fmt.Sprint(value))
		}
	}

	key = strings.ReplaceAll(key, "{", "")
	key = strings.ReplaceAll(key, "}", "")
	return key, nil
}

func (k *KafkaOutput) OnEvent(ctx context.Context, params *stream.Event) error {
	k.bulk.Add(util.BulkItem[stream.Event]{Data: *params, Type: params.Topic, Size: len(params.Datas)})
	return nil
}
func (k *KafkaOutput) Run() {
	k.bulk.Start()

	ctx := context.Background()
	go func() {
		defer k.DoneEnd()
		doneSig := k.DoneBegin()
		for {
			select {
			case <-doneSig:
				k.bulk.Stop()
				k.flushRemainingData(ctx)
				return
			case batch := <-k.dataCh:
				if err := k.processBatch(ctx, batch); err != nil {
					k.logger.Error().Err(err).Msg("failed to process batch")
				}
			}
		}
	}()
}
func (k *KafkaOutput) processBatch(ctx context.Context, batch []util.BulkItem[stream.Event]) error {

	params := []stream.Event{}
	for _, item := range batch {
		params = append(params, item.Data)
	}
	if len(params) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, len(params))
	for idx, param := range params {
		data := map[string]interface{}{
			"Topic": param.Topic,
			"Datas": param.Datas,
		}
		buf, err := json.Marshal(data)
		if err != nil {
			k.logger.Error().Err(err).Any("topic", k.config.Topic).Any("value topic", param.Topic).Msgf("marshal %v ", data)
			return err
		}
		key := k.config.Key
		if key == "" && k.config.KeyTemplate != "" {
			// key = generateKey(params.Datas[],k.config.KeyTemplate)
		}
		msgs[idx] = kafka.Message{
			Key:   []byte(k.config.Key),
			Value: buf,
		}
	}
	err := k.producer.WriteMessages(ctx, msgs...)
	if err != nil {
		k.logger.Error().Err(err).Any("topic", k.config.Topic).Any("message count", len(msgs)).Msg("failed ")
	} else {
		k.logger.Info().Any("topic", k.config.Topic).Any("write message count", len(msgs)).Msg("success")
	}
	return err
}

func (k *KafkaOutput) flushRemainingData(ctx context.Context) {
	k.logger.Info().Msgf("kafka flush remaining data")
	select {
	case remainingBatch := <-k.dataCh:
		if err := k.processBatch(ctx, remainingBatch); err != nil {
			k.logger.Error().Err(err).Msg("error flushing remaining data to kafka")
		}
	default:
		// 无剩余数据
	}
}
