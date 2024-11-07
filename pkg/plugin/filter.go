package plugin

import (
	"context"
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/stream"
)

type FilterConfig struct {
	BaseConfig `yaml:",inline"`
}

type Filter struct {
	BasePlugin
	filter handler.Matcher
	cfg    *FilterConfig
}

func NewFilterr(base BasePlugin, cfg *FilterConfig) (*Filter, error) {
	return &Filter{BasePlugin: base, cfg: cfg}, nil
}

func (r Filter) OnEvent(ctx context.Context, event *stream.Event) error {
	return nil
}

func (f *Filter) Match(ctx context.Context, event *stream.Event) (out *stream.Event) {
	out = &stream.Event{
		Context: event.Context,
		Topic:   event.Topic,
	}
	hasKeyIngrex, matchd := f.BasePlugin.MatchIngrex(ctx, event)
	if hasKeyIngrex && matchd {
		event.Datas = event.Datas[:0]
		return out
	}
	results := []map[string]interface{}{}
	for _, data := range event.Datas {
		if !f.BasePlugin.MatchData(ctx, data) {
			results = append(results, data)
		}
	}
	event.Datas = results
	return out
}
