package plugin

import (
	"context"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util/jsonpath"
)

type RenameConfig struct {
	BaseConfig `yaml:",inline"`
	Names      map[string]string `yaml:"names"`
}

type Rename struct {
	BasePlugin
	names map[string]string
}

func NewRename(base BasePlugin, cfg *RenameConfig) (*Rename, error) {
	return &Rename{BasePlugin: base, names: cfg.Names}, nil
}

func (r Rename) OnEvent(ctx context.Context, event *stream.Event) error {
	if len(r.names) > 0 {
		for _, data := range event.Datas {
			for name, to := range r.names {
				jsonpath.Rename(data, name, to)
			}
		}
	}
	return nil
}
