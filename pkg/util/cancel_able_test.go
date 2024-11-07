package util

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCancelable(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	cancelable := NewCancelable(parentCtx)

	go func() {
		<-cancelable.DoneBegin()
		fmt.Println("子上下文收到取消信号")
		cancelable.DoneEnd()
	}()

	time.Sleep(2 * time.Second)
	fmt.Println("父上下文取消")
	parentCancel()

	time.Sleep(1 * time.Second)
}
