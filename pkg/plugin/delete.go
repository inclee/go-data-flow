package plugin

import (
	"context"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util/jsonpath"
)

type DeleteConfig struct {
	BaseConfig `yaml:",inline"`
	Fields     []string `yaml:"fields"`
}

type Deleter struct {
	BasePlugin
	fields []string
}

func NewDeleter(base BasePlugin, cfg *DeleteConfig) (*Deleter, error) {
	return &Deleter{BasePlugin: base, fields: cfg.Fields}, nil
}

func (r Deleter) OnEvent(ctx context.Context, event *stream.Event) error {
	if len(r.fields) > 0 {
		for _, filed := range r.fields {
			for _, data := range event.Datas {
				jsonpath.Delete(data, filed)
			}
		}
	}
	return nil
}
