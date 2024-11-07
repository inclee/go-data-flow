package output

import (
	"context"
	"encoding/json"
	"fmt"
	"go-data-flow/pkg/stream"
)

type StdOut struct {
	BaseOutput
}

func NewStdOut(base BaseOutput) *StdOut {
	st := &StdOut{BaseOutput: base}
	return st
}

func (st *StdOut) OnEvent(ctx context.Context, params *stream.Event) error {
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
