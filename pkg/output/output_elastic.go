package output

import (
	"context"
	"fmt"
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/logs"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util"
	"regexp"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ElasticConfig struct {
	Url               string              `yaml:"url"`
	User              string              `yaml:"user"`
	Pass              string              `yaml:"pass"`
	IndexTableMapping map[string][]string `yaml:"index_table_mapping"`
	BulkSize          int                 `yaml:"bulk_size"`
	BulkFlushSec      int                 `yaml:"bulk_flush_sec"`
}

type ElasticOutput struct {
	BaseOutput
	client     *elastic.Client
	cfg        *ElasticConfig
	indexregx  map[string][]*regexp.Regexp
	indexcache map[string]string
	bulk       *util.Bulk[stream.Event]
	dataCh     chan []util.BulkItem[stream.Event]
	logger     zerolog.Logger
}

func NewElasticOutput(base BaseOutput, cfg *ElasticConfig) (*ElasticOutput, error) {
	// 初始化Elastic
	baseLogger := log.With().Any(logs.Output, "ElasticSearch").Logger()
	elsOpts := make([]elastic.ClientOptionFunc, 0, 10)
	elsOpts = append(elsOpts, elastic.SetErrorLog(&baseLogger))
	elsOpts = append(elsOpts, elastic.SetURL(cfg.Url))
	elsOpts = append(elsOpts, elastic.SetSniff(false))
	elsOpts = append(elsOpts, elastic.SetHealthcheck(false))
	if len(cfg.User) > 0 && len(cfg.Pass) > 0 {
		elsOpts = append(elsOpts, elastic.SetBasicAuth(cfg.User, cfg.Pass))
	}
	client, err := elastic.NewClient(elsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	regxmap := map[string][]*regexp.Regexp{}
	for index, tables := range cfg.IndexTableMapping {
		regxmap[index] = make([]*regexp.Regexp, 0)
		for _, table := range tables {
			regex, err := regexp.Compile(table)
			if err != nil {
				return nil, err
			}
			regxmap[index] = append(regxmap[index], regex)
		}

	}
	dataCh := make(chan []util.BulkItem[stream.Event])
	bulk := util.NewBulk(cfg.BulkSize, time.Duration(cfg.BulkFlushSec)*time.Second, dataCh)

	output := &ElasticOutput{
		BaseOutput: base,
		client:     client,
		cfg:        cfg,
		indexregx:  regxmap,
		indexcache: map[string]string{},
		bulk:       bulk,
		dataCh:     dataCh,
		logger:     baseLogger,
	}
	output.Run()
	return output, nil
}

func (es *ElasticOutput) OnEvent(ctx context.Context, params *stream.Event) error {
	es.bulk.Add(util.BulkItem[stream.Event]{Data: *params, Type: params.Topic, Size: len(params.Datas)})
	return nil
}

func (es *ElasticOutput) Run() {
	es.bulk.Start()
	ctx := context.Background()
	go func() {
		defer es.DoneEnd()
		doneSig := es.DoneBegin()
		for {
			select {
			case <-doneSig:
				es.bulk.Stop()
				es.flushRemainingData(ctx)
				return
			case batch := <-es.dataCh:
				if err := es.processBatch(ctx, batch); err != nil {
					es.logger.Error().Err(err).Msg("failed to process batch")
				}
			}
		}
	}()
}

func (es *ElasticOutput) processBatch(ctx context.Context, batch []util.BulkItem[stream.Event]) error {
	params := []stream.Event{}
	for _, item := range batch {
		params = append(params, item.Data)
	}
	if len(params) == 0 {
		return nil
	}
	batchs := map[string]map[string][]map[string]interface{}{}
	for _, param := range params {
		for _, data := range param.Datas {
			action := data["action"].(string)
			typ := data["type"].(string)
			msgs := []map[string]interface{}{}
			switch data["messages"].(type) {
			case []map[string]interface{}:
				msgs = data["messages"].([]map[string]interface{})
			case []interface{}:
				for _, msg := range data["messages"].([]interface{}) {
					msgs = append(msgs, msg.(map[string]interface{}))
				}
			default:
				es.logger.Error().Err(fmt.Errorf("消息类型错误")).Any("action", action).Any("type", typ).Msg("error writing batch to Elasticsearch")
			}

			index := ""
			if index = es.indexcache[typ]; index == "" {
				for idx, regxs := range es.indexregx {
					for _, regx := range regxs {
						if regx.MatchString(typ) {
							index = idx
							es.indexcache[typ] = index
							break
						}
					}
				}
				if index == "" {
					return fmt.Errorf("can't find elasetic index for message %s.%s", param.Topic, typ)
				}
			}
			if _, ok := batchs[index]; !ok {
				batchs[index] = map[string][]map[string]interface{}{}
			}
			batchs[index][action] = append(batchs[index][action], msgs...)
		}
	}
	// 写入批次数据
	for index, actions := range batchs {
		for action, messages := range actions {
			if err := es.writeout(ctx, index, action, messages); err != nil {
				es.logger.Error().Err(err).Str("index", index).Msg("error writing batch to Elasticsearch")
				return err
			}
			es.logger.Info().Int("actions", len(messages)).Str("index", index).Msg("bulk request executed successfully")
		}
	}
	return nil
}

func (es *ElasticOutput) flushRemainingData(ctx context.Context) {
	es.logger.Info().Msg("elasticOutput flush remaining data")
	select {
	case remainingBatch := <-es.dataCh:
		if err := es.processBatch(ctx, remainingBatch); err != nil {
			es.logger.Error().Err(err).Msg("error flushing remaining data to elasticsearch")
		}
	default:
		// 无剩余数据
	}
	es.logger.Info().Msg("elasticOutput flush remaining data end")
}

func (es *ElasticOutput) writeout(ctx context.Context, index, action string, msgs []map[string]interface{}) error {
	if len(msgs) == 0 {
		return nil
	}
	bulkRequest := es.client.Bulk()

	for _, msg := range msgs {
		if msg["id"] == nil {
			return fmt.Errorf("%s has no id filed", index)
		}
		id := fmt.Sprintf("%v", msg["id"]) // 如果没有ID，需要rename或者combine出唯一ID字段
		var req elastic.BulkableRequest

		switch handler.EventType(action) {
		case handler.InsertEvent:
			req = elastic.NewBulkCreateRequest().Index(index).Id(id).Doc(msg)
		case handler.UpdateEvent:
			req = elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(msg)
		case handler.DeleteEvent:
			req = elastic.NewBulkDeleteRequest().Index(index).Id(id)
		default:
			return fmt.Errorf("unsupported event type: %v", action)
		}
		bulkRequest = bulkRequest.Add(req)

	}

	if bulkRequest.NumberOfActions() == 0 {
		return nil
	}

	_, err := bulkRequest.Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	return nil
}
