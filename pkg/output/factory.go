package output

import (
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/plugin"
	"go-data-flow/pkg/util"
	"reflect"
)

func init() {
	RegisterFactory("elastic", func(base BaseOutput, cfg interface{}) (Output, error) {
		return NewElasticOutput(base, cfg.(*ElasticConfig))
	})
	RegisterFactory("stdout", func(base BaseOutput, cfg interface{}) (Output, error) {
		return NewStdOut(base), nil
	})
	RegisterFactory("kafka", func(base BaseOutput, cfg interface{}) (Output, error) {
		return NewKafkaOutput(base, cfg.(*KafkaOutputConfig))
	})
}

type Config struct {
	Plugins []*plugin.Config   `yaml:"plugins"`
	Elastic *ElasticConfig     `yaml:"elastic"`
	Kafka   *KafkaOutputConfig `yaml:"kafka"`
	Stdout  *struct{}          `yaml:"stdout"`
}

func RegisterFactory(name string, factory OutputFactory) {
	if name == "" || factory == nil {
		return
	}
	factories[name] = factory
}

type OutputFactory func(base BaseOutput, cfg interface{}) (Output, error)

var factories = make(map[string]OutputFactory)

func Factory(cancelable *util.Cancelable, cfg Config) (handler.Handler, error) {
	link := &handler.LinkHandler{}
	for _, plugins := range cfg.Plugins {
		pitem, err := plugin.Factory(*plugins)
		if err != nil {
			return nil, err
		}
		link.Append(pitem)
	}
	v := reflect.ValueOf(cfg)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := v.Type().Field(i)
		if field.Kind() == reflect.Ptr && field.IsNil() {
			continue
		}
		factory, exists := factories[fieldType.Tag.Get("yaml")]
		if exists {
			matcher, err := handler.NewDefaultMatcher(handler.GetMatchConfig(field))
			if err != nil {
				return nil, err
			}
			oitem, err := factory(BaseOutput{Cancelable: cancelable, Matcher: matcher}, field.Interface())
			if err != nil {
				return nil, err
			}
			link.Append(oitem)
		}
	}
	return link, nil
}
