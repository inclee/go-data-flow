package input

import (
	"context"
	"go-data-flow/pkg/command"
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/input/canal"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util"
	"reflect"
)

type Input interface {
	Flow(context.Context) *stream.Scream
}

type BaseInput struct {
	*util.Cancelable
	handler.Matcher
	commander *command.Commander
}

func NewInput(cancelable *util.Cancelable, cfg Config, commander *command.Commander) (Input, error) {
	return Factory(cancelable, cfg, commander)
}

func init() {
	RegisterFactory("canal", func(base BaseInput, cfg interface{}) (Input, error) {
		return NewCanal(base, cfg.(*canal.Config))
	})
	RegisterFactory("kafka", func(base BaseInput, cfg interface{}) (Input, error) {
		return NewKafkaInput(base, cfg.(*KafkaInputConfig))
	})
}

func RegisterFactory(name string, factory OutputFactory) {
	if name == "" || factory == nil {
		return
	}
	factories[name] = factory
}

type OutputFactory func(base BaseInput, cfg interface{}) (Input, error)

var factories = make(map[string]OutputFactory)

func Factory(cancelable *util.Cancelable, cfg Config, commander *command.Commander) (Input, error) {
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
			input, err := factory(BaseInput{Cancelable: cancelable, Matcher: matcher, commander: commander}, field.Interface())
			if err != nil {
				return nil, err
			}
			return input, nil
		}
	}
	return nil, nil
}
