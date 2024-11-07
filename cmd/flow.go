package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"go-data-flow/internal/config"
	"go-data-flow/pkg/command"
	"go-data-flow/pkg/flow"
	"go-data-flow/pkg/redis"
	"go-data-flow/pkg/util"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var syncCmd = &cobra.Command{
	Use:   "flow",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second * 10)
			ok, err := redis.Ins.SetNX(config.Cfg.DaemonKey, 1, 10*time.Second).Result()
			if err != nil {
				log.Err(err).Msgf(fmt.Sprintf("set redis nx %s error:%s", config.Cfg.DaemonKey, err))
				continue
			}
			if !ok {
				err := fmt.Errorf("%s process exist", config.Cfg.DaemonKey)
				log.Err(err).Msgf(fmt.Sprintf("set redis nx %s error:%s", config.Cfg.DaemonKey, err))
				continue
			}
			break
		}

		defer func() {
			redis.Ins.Del(config.Cfg.DaemonKey)
		}()
		go func() {
			ticker := time.Tick(5 * time.Second)
			for range ticker {
				redis.Ins.Set(config.Cfg.DaemonKey, 1, 10*time.Second)
			}
		}()

		gs := util.NewGracefulShutdown(context.Background())
		errc := make(chan error)
		go func() {
			for err := range errc {
				util.MailTo(config.Cfg.Mail.Server, fmt.Sprintf("%s:%s", config.Cfg.DaemonKey, "Data Flow Error Alert"), err.Error(), config.Cfg.Mail.To, nil)
			}
		}()

		commander := command.NewCommander()
		for _, fcfg := range config.Cfg.Flows {
			fitem, err := flow.NewFlow(gs.Cancelable, fcfg, commander)
			if err != nil {
				gs.Cancel()
				log.Err(err).Msgf(fmt.Sprintf("run flow error %+v", fitem))
				return
			}
			go func() {
				fitem.Run(gs.Context(), errc)
			}()
		}

		startHTTPServer(gs, commander)
		gs.ListenForShutdown()
	},
}

func startHTTPServer(gs *util.GracefulShutdown, commander *command.Commander) error {
	ginEngine := gin.Default()
	ginEngine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	ginEngine.POST("/cmd", func(ctx *gin.Context) {
		var request struct {
			Module string          `json:"module"`
			Cmd    string          `json:"cmd"`
			Param  json.RawMessage `json:"params"`
		}
		if err := ctx.Bind(&request); err != nil {
			ctx.JSON(http.StatusBadRequest, "参数错误")
			return
		}
		resp, err := commander.OnHandler(request.Module, request.Cmd, request.Param)
		if err != nil {
			ctx.JSON(http.StatusOK, fmt.Sprintf("命令执行失败：%s", err))
			return
		}
		ctx.JSON(http.StatusOK, fmt.Sprintf("命令执行成功：%v", resp))
		return
	})
	go func() {
		if err := ginEngine.Run(config.Cfg.Http.Addr); err != nil {
			log.Error().Msgf("HTTP 服务启动失败: %v", err)
			gs.Cancel()
		}
	}()
	return nil
}

func init() {
	Root.AddCommand(syncCmd)
}
