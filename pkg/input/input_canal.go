package input

import (
	"context"
	"encoding/json"
	"go-data-flow/pkg/input/canal"
	"go-data-flow/pkg/redis"
	"go-data-flow/pkg/stream"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type Canal struct {
	BaseInput
	ins    *canal.Canal
	stream *stream.Scream
	cfg    *canal.Config
}

func NewCanal(base BaseInput, cfg *canal.Config) (*Canal, error) {
	stream := stream.NewSteam()
	ins, err := canal.NewCanal(cfg, canal.NewRedisPosSaver(redis.Ins, "flow", cfg.Addr), stream)
	if err != nil {
		return nil, err
	}
	canal := &Canal{BaseInput: base, ins: ins, stream: stream, cfg: cfg}
	canal.registerCommand()
	return canal, nil
}

func (c *Canal) Flow(ctx context.Context) *stream.Scream {
	go func() {
		err := c.ins.Run(ctx)
		c.stream.Err <- err
	}()
	go func() {
		select {
		case <-c.Context().Done():
			c.ins.Close()
		}
	}()
	return c.stream
}

func (c *Canal) registerCommand() {
	c.commander.RegisterHandler("canal", "resync_tables", c.resyncTables)
	c.commander.RegisterHandler("canal", "sync_from_position", c.syncFromPosition)
}

func (c *Canal) resyncTables(req json.RawMessage) (ok bool, resp interface{}, err error) {
	var request struct {
		Tables     []string `json:"tables"`
		RegxTables []string `json:"regx_tables"`
	}
	json.Unmarshal(req, &request)
	if len(request.Tables) == 0 && len(request.RegxTables) == 0 {
		resp = "表名称不能为空"
		return true, resp, nil
	}
	ok, err = c.ins.ResyncTables(c.Context(), request.Tables)
	if err != nil {
		return ok, "", err
	}
	regOk, err := c.ins.ResyncTablesRegx(c.Context(), request.RegxTables)
	ok = ok || regOk
	resp = ""
	if err == nil {
		resp = "同步命令已提交"
	}
	return ok, resp, err
}

func (c *Canal) syncFromPosition(req json.RawMessage) (ok bool, resp interface{}, err error) {
	var request struct {
		Addr string         `json:"addr"`
		Pos  mysql.Position `json:"position"`
	}
	json.Unmarshal(req, &request)
	if len(request.Addr) == 0 {
		resp = "Canal连接不能为空"
		return true, resp, nil
	}
	if len(request.Pos.Name) == 0 || request.Pos.Pos == 0 {
		resp = "binlog位置配置错误"
		return true, resp, nil
	}
	if request.Addr == c.cfg.Addr {
		ok = true
		err = c.ins.SyncFromPos(request.Pos)
		if err == nil {
			resp = "Command submitted successfully"
		}
	}
	return
}
