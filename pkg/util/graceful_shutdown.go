package util

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

type GracefulShutdown struct {
	*Cancelable
}

func NewGracefulShutdown(ctx context.Context) *GracefulShutdown {
	return &GracefulShutdown{
		Cancelable: NewCancelable(ctx),
	}
}

func (gs *GracefulShutdown) ListenForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Info().Msgf("received signal %v for shutdown...", sig.String())
	case <-gs.Context().Done():
		log.Info().Msg("context canceled for shutdown...")
	}

	gs.Shutdown()
}

func (gs *GracefulShutdown) Shutdown() {
	log.Info().Msg("shutdown...")
	gs.Cancel() // 取消所有子任务
	gs.Wait()   // 等待子任务完成
	log.Info().Msg("shutdown complete.")
}
