package output

import (
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/util"
)

type Output interface {
	handler.Handler
	handler.Matcher
}

func Outputs(cancelable *util.Cancelable, cfgs []Config) (*handler.LinkHandler, error) {
	link := &handler.LinkHandler{Head: true}
	for _, cfg := range cfgs {
		output, err := Factory(cancelable, cfg)
		if err != nil {
			return link, err
		}
		link.Append(output)
	}
	return link, nil
}

type BaseOutput struct {
	*util.Cancelable
	handler.Matcher
}
