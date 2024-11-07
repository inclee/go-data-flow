package handler

import (
	"context"
	"go-data-flow/pkg/stream"

	"github.com/mohae/deepcopy"
)

type LinkHandler struct {
	list []Handler
	Head bool
}

func (link *LinkHandler) Append(item Handler) {
	link.list = append(link.list, item)
}

func (link *LinkHandler) OnEvent(ctx context.Context, event *stream.Event) error {
	var err error
	errChan := make(chan error, len(link.list)) // 使用缓冲通道收集错误
	for _, item := range link.list {
		if link.Head {
			err = item.OnEvent(ctx, deepcopy.Copy(event).(*stream.Event))
		} else {
			err = item.OnEvent(ctx, item.Match(ctx, event))
		}

		if err != nil {
			errChan <- err
		}
	}
	close(errChan)

	for e := range errChan {
		return e
	}

	return nil
}

func (link *LinkHandler) Match(ctx context.Context, event *stream.Event) *stream.Event {
	return event
}

func (link *LinkHandler) MatchIngrex(ctx context.Context, event *stream.Event) (bool, bool) {
	return false, true
}

func (link *LinkHandler) MatchData(ctx context.Context, data map[string]interface{}) bool {
	return true
}
