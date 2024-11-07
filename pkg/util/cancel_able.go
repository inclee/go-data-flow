package util

import (
	"context"
	"sync"
)

type Cancelable struct {
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup
}

func NewCancelable(parentCtx context.Context) *Cancelable {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Cancelable{
		ctx:       ctx,
		cancel:    cancel,
		waitGroup: sync.WaitGroup{},
	}
}

func (c *Cancelable) Context() context.Context {
	return c.ctx
}

func (c *Cancelable) Cancel() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Cancelable) DoneBegin() <-chan struct{} {
	c.waitGroup.Add(1)
	return c.ctx.Done()
}

func (c *Cancelable) DoneEnd() {
	c.waitGroup.Done()
}

func (c *Cancelable) Wait() {
	c.waitGroup.Wait()
}
