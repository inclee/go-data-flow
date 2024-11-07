package handler

import (
	"context"
	"errors"
	"go-data-flow/pkg/stream"

	"github.com/go-mysql-org/go-mysql/canal"
)

// 定义事件类型
type EventType string

// 定义可能的事件类型常量
const (
	InsertEvent  EventType = "insert"
	UpdateEvent  EventType = "update"
	DeleteEvent  EventType = "delete"
	InvalidEvent EventType = "invalid"
)

// 定义事件映射表，将 canal 库的事件映射为自定义事件类型
var eventMap = map[string]EventType{
	canal.InsertAction: InsertEvent,
	canal.UpdateAction: UpdateEvent,
	canal.DeleteAction: DeleteEvent,
}

// Event 根据 canal 库的事件类型获取自定义事件类型
func Event(canalEvent string) EventType {
	event, ok := eventMap[canalEvent]
	if !ok {
		return InvalidEvent
	}
	return event
}

// EventParams 定义传递事件参数的结构体
type EventParams struct {
	EventType EventType
	Schema    string
	Table     string
	Rows      []map[string]interface{}
}

// 定义 Handler 接口，处理事件
type Handler interface {
	Matcher
	OnEvent(ctx context.Context, params *stream.Event) error
}

// ErrInvalidEvent 定义无效事件的错误
var ErrInvalidEvent = errors.New("invalid event type")

// NewEventParams 创建 EventParams，并且验证 eventType
func NewEventParams(canalEvent string, schema, table string, rows []map[string]interface{}) (EventParams, error) {
	eventType := Event(canalEvent)
	if eventType == InvalidEvent {
		return EventParams{}, ErrInvalidEvent
	}

	return EventParams{
		EventType: eventType,
		Schema:    schema,
		Table:     table,
		Rows:      rows,
	}, nil
}
