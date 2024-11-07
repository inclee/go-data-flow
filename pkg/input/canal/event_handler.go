package canal

import (
	"context"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/rs/zerolog/log"
)

type eventHandler struct {
	filterAction map[string]bool
	canal        *Canal
}

func NewEventHandler(canal *Canal) *eventHandler {
	filterActs := map[string]bool{}
	for _, action := range canal.cfg.FilterActions {
		filterActs[action] = true
	}
	return &eventHandler{canal: canal, filterAction: filterActs}
}

func (h *eventHandler) OnRotate(_ *replication.EventHeader, e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	log.Debug().
		Str("next_log_name", pos.Name).
		Uint32("position", pos.Pos).
		Msg("Handling Rotate Event")

	return h.canal.savePos(pos)
}

func (h *eventHandler) OnTableChanged(_ *replication.EventHeader, schema, table string) error {
	log.Debug().
		Str("schema", schema).
		Str("table", table).
		Msg("Handling Table Changed Event")

	h.canal.OnTableChanged(schema, table)
	return nil
}

func (h *eventHandler) OnDDL(_ *replication.EventHeader, nextPos mysql.Position, _ *replication.QueryEvent) error {
	log.Debug().
		Str("binlog_name", nextPos.Name).
		Uint32("position", nextPos.Pos).
		Msg("Handling DDL Event")

	return h.canal.savePos(nextPos)
}

func (h *eventHandler) OnXID(_ *replication.EventHeader, nextPos mysql.Position) error {
	log.Debug().
		Str("binlog_name", nextPos.Name).
		Uint32("position", nextPos.Pos).
		Msg("Handling XID (Transaction Commit) Event")

	return h.canal.savePos(nextPos)
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	log.Debug().
		Str("schema", e.Table.Schema).
		Str("table", e.Table.Name).
		Str("action", e.Action).
		Int("rows_count", len(e.Rows)).
		Msg("Handling Row Change Event")

	if h.filterAction[e.Action] {
		return nil
	}
	return h.canal.OnEvent(context.Background(), e.Table.Schema, e.Table.Name, e.Action, e.Rows)
}

func (h *eventHandler) OnGTID(_ *replication.EventHeader, gtid mysql.BinlogGTIDEvent) error {
	// log.Info().Msg("GTID Event Triggered (no-op)")
	return nil
}

func (h *eventHandler) OnPosSynced(_ *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// log.Info().Msg("PosSynced Event Triggered (no-op)")
	return nil
}

func (h *eventHandler) String() string {
	return "BinlogEventHandler"
}
func (h *eventHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}
