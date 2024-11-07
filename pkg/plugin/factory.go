package plugin

import (
	"fmt"
	"go-data-flow/pkg/handler"
	"reflect"
)

func wrapFactory[BaseT any, C any, R any](ctor func(BaseT, *C) (R, error)) func(BaseT, interface{}) (interface{}, error) {
	return func(base BaseT, cfg interface{}) (interface{}, error) {
		config, ok := cfg.(*C)
		if !ok {
			return nil, fmt.Errorf("invalid config type: expected *%T, got %T", config, cfg)
		}
		return ctor(base, config)
	}
}

var pluginFactories = map[string]struct {
	ctor    func(BasePlugin, interface{}) (interface{}, error)
	cfgType reflect.Type
}{
	"rename":  {wrapFactory(NewRename), reflect.TypeOf(&RenameConfig{})},
	"combine": {wrapFactory(NewCombiner), reflect.TypeOf(&CombineConfig{})},
	"delete":  {wrapFactory(NewDeleter), reflect.TypeOf(&DeleteConfig{})},
	"filter":  {wrapFactory(NewFilterr), reflect.TypeOf(&FilterConfig{})},
}

func init() {
	for name, factory := range pluginFactories {
		localName := name
		localFactory := factory
		RegisterFactory(localName, func(base BasePlugin, cfg interface{}) (interface{}, error) {
			cfgVal := reflect.ValueOf(cfg)
			if cfgVal.Type() != localFactory.cfgType {
				return nil, fmt.Errorf("invalid config type for %s: expected %s, got %s", localName, localFactory.cfgType, cfgVal.Type())
			}
			return localFactory.ctor(base, cfg)
		})
	}
}

type Config struct {
	Rename  *RenameConfig  `yaml:"rename"`
	Combine *CombineConfig `yaml:"combine"`
	Delete  *DeleteConfig  `yaml:"delete"`
	Filter  *FilterConfig  `yaml:"filter"`
}

type PluginFactory func(base BasePlugin, cfg interface{}) (interface{}, error)

var factories = make(map[string]PluginFactory)

func RegisterFactory(name string, factory PluginFactory) {
	factories[name] = factory
}

func Factory(cfg Config) (handler.Handler, error) {
	link := &handler.LinkHandler{}
	v := reflect.ValueOf(cfg)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := v.Type().Field(i)
		if field.Kind() == reflect.Ptr && !field.IsNil() {
			factory, exists := factories[fieldType.Tag.Get("yaml")]
			if exists {
				matcher, err := handler.NewDefaultMatcher(handler.GetMatchConfig(field))
				if err != nil {
					return nil, err
				}
				oitem, err := factory(BasePlugin{Matcher: matcher}, field.Interface())
				if err != nil {
					return nil, err
				}
				link.Append(oitem.(handler.Handler))
			}
		}
	}
	return link, nil
}
