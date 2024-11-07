package flow

import (
	"context"
	"go-data-flow/pkg/command"
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/input"
	"go-data-flow/pkg/output"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util"
	"sync"
)

type Config struct {
	Input     input.Config    `yaml:"input"`
	Outputs   []output.Config `yaml:"outputs"`
	WorkCount int             `yaml:"work_count"`
}

type Flow struct {
	input  input.Input
	output handler.Handler
	worker int
}

func NewFlow(cancelable *util.Cancelable, cfg Config, commander *command.Commander) (*Flow, error) {
	outputs, err := output.Outputs(cancelable, cfg.Outputs)
	if err != nil {
		return nil, err
	}
	input, err := input.NewInput(cancelable, cfg.Input, commander)
	if err != nil {
		return nil, err
	}
	worker := cfg.WorkCount
	if worker == 0 {
		worker = 1
	}
	return &Flow{
		input:  input,
		output: outputs,
		worker: worker,
	}, nil
}

func (f *Flow) Run(ctx context.Context, errc chan error) {
	var wg sync.WaitGroup
	for i := 0; i < f.worker; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			flow := f.input.Flow(ctx)
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-flow.Err:
					errc <- err
				case event, ok := <-flow.In:
					if !ok {
						return
					}
					flow.Out <- stream.EventResult{Result: 0, Error: f.output.OnEvent(ctx, &event)}
				}
			}
		}()
	}
	wg.Wait()
	return
}
