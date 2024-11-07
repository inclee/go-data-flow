package util

import (
	"context"
	"sync"
	"time"
)

type BulkItem[T any] struct {
	Data T
	Size int
	Type string // 用于区分不同类型的数据
}

type Bulk[T any] struct {
	sync.Mutex
	buffers      map[string][]BulkItem[T] // 基于 Type 分类的缓冲
	bufferSize   map[string]int           // 根据 Type 累计size
	maxSize      int                      // 每个类型的最大缓冲大小
	flushTimeout time.Duration            // 每个类型的超时时间
	dataCh       chan []BulkItem[T]       // 批量数据发送通道
	ticker       *time.Ticker             // 定时器
	ctx          context.Context          // 控制优雅退出
	cancel       context.CancelFunc
}

func NewBulk[T any](maxSize int, flushTimeout time.Duration, dataCh chan []BulkItem[T]) *Bulk[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Bulk[T]{
		buffers:      make(map[string][]BulkItem[T]),
		bufferSize:   map[string]int{},
		maxSize:      maxSize,
		flushTimeout: flushTimeout,
		dataCh:       dataCh,
		ticker:       time.NewTicker(flushTimeout),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (bulk *Bulk[T]) Add(item BulkItem[T]) {
	bulk.Lock()
	defer bulk.Unlock()

	bulk.buffers[item.Type] = append(bulk.buffers[item.Type], item)
	bulk.bufferSize[item.Type] += item.Size

	if bulk.bufferSize[item.Type] >= bulk.maxSize {
		bulk.flush(item.Type)
	}
}

func (bulk *Bulk[T]) flush(itemType string) {
	if len(bulk.buffers[itemType]) == 0 {
		return
	}

	bulk.dataCh <- bulk.buffers[itemType]

	bulk.buffers[itemType] = make([]BulkItem[T], 0, bulk.maxSize)
	bulk.bufferSize[itemType] = 0
}

func (bulk *Bulk[T]) Start() {
	go func() {
		for {
			select {
			case <-bulk.ticker.C:
				bulk.Lock()
				for itemType := range bulk.buffers {
					bulk.flush(itemType)
				}
				bulk.Unlock()
			case <-bulk.ctx.Done():
				bulk.Lock()
				for itemType := range bulk.buffers {
					bulk.flush(itemType)
				}
				close(bulk.dataCh)
				bulk.Unlock()
				return
			}
		}
	}()
}

func (bulk *Bulk[T]) Stop() {
	bulk.ticker.Stop()
	bulk.cancel()
}

func (bulk *Bulk[T]) Close() {
	bulk.Stop()
}
