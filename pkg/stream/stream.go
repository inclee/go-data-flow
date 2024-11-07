package stream

import (
	"context"
)

type Event struct {
	Context context.Context
	Topic   string
	Datas   []map[string]interface{}
}

type EventResult struct {
	Result interface{}
	Error  error
}
type Scream struct {
	In  chan Event
	Out chan EventResult
	Err chan error
}

func NewSteam() *Scream {
	return &Scream{
		In:  make(chan Event),
		Out: make(chan EventResult),
		Err: make(chan error),
	}
}
