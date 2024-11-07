package plugin

import "go-data-flow/pkg/handler"

type Plugin interface {
	handler.Handler
	handler.Matcher
}

type BaseConfig struct {
	Match handler.MatchConfig `yaml:"match"`
}
type BasePlugin struct {
	handler.Matcher
}
