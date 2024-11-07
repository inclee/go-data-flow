package plugin

import (
	"context"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util/jsonpath"
)

type CombineConfig struct {
	BaseConfig `yaml:",inline"`
	Fields     map[string][]string `yaml:"fields"`
	Join       string              `yaml:"join"`
}

type Combine struct {
	BasePlugin
	cfg *CombineConfig
}

func NewCombiner(base BasePlugin, cfg *CombineConfig) (*Combine, error) {
	return &Combine{BasePlugin: base, cfg: cfg}, nil
}

func (r Combine) OnEvent(ctx context.Context, event *stream.Event) error {
	if len(r.cfg.Fields) > 0 {
		for _, data := range event.Datas {
			for field, refs := range r.cfg.Fields {
				jsonpath.InsertField(data, refs, field, r.cfg.Join)
			}
		}
	}
	return nil
}
