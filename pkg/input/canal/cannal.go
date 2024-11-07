package canal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-data-flow/pkg/handler"
	"go-data-flow/pkg/logs"
	"go-data-flow/pkg/stream"
	"go-data-flow/pkg/util/containers/slices"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Canal struct {
	cli           *canal.Canal
	db            *sql.DB
	cfg           *Config
	stream        *stream.Scream
	posSaver      PosSaver
	tables        map[string]*schema.Table
	isIncremental int32
	incrementCond *sync.Cond
	restart       chan int
	logger        zerolog.Logger
}

type SyncType int

const (
	SyncIncrement SyncType = iota
	SyncAll
)

type Config struct {
	Addr              string   `yaml:"addr"`
	User              string   `yaml:"user"`
	Password          string   `yaml:"password"`
	ServerID          uint32   `yaml:"server_id"`
	IncludeTableRegex []string `yaml:"include_table_regex"`
	ExcludeTableRegex []string `yaml:"exclude_table_regex"`
	MonitorInter      int      `yaml:"monitor_inter"`
	DelayPos          int      `yaml:"delay_pos"`
	FilterActions     []string `yaml:"filter_actions"`
	FullSyncPageSize  int      `yaml:"full_sync_page_size"`
}

func NewCanal(cfg *Config, posSaver PosSaver, stream *stream.Scream) (*Canal, error) {
	if cfg.DelayPos == 0 {
		cfg.DelayPos = 1000000
	}
	if cfg.FullSyncPageSize == 0 {
		cfg.FullSyncPageSize = 1000
	}

	// 初始化 MySQL 连接
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", cfg.User, cfg.Password, cfg.Addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	ret := &Canal{
		db:            db,
		cfg:           cfg,
		posSaver:      posSaver,
		stream:        stream,
		tables:        make(map[string]*schema.Table),
		isIncremental: 1,                           // 初始状态允许增量同步
		incrementCond: sync.NewCond(&sync.Mutex{}), // 条件变量用于控制全量同步
		restart:       make(chan int),
		logger:        log.With().Any(logs.Input, "Canal").Logger(),
	}

	return ret, nil
}

func (c *Canal) getCanalInstance() (*canal.Canal, error) {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = c.cfg.Addr
	canalCfg.User = c.cfg.User
	canalCfg.Password = c.cfg.Password
	canalCfg.ServerID = c.cfg.ServerID
	canalCfg.IncludeTableRegex = c.cfg.IncludeTableRegex
	canalCfg.Dump.ExecutionPath = ""
	return canal.NewCanal(canalCfg)
}

func (c *Canal) Run(ctx context.Context) (err error) {
	tryCnt := 0
	backoff := time.Second
	for ctx.Err() == nil {
		//延迟创建
		cli, err := c.getCanalInstance()
		if err != nil {
			return fmt.Errorf("create canal failed %w", err)
		}
		c.cli = cli
		lastpos, err := c.posSaver.Get()
		if err != nil {
			return fmt.Errorf("get last canal pos failed: %w", err)
		}

		c.cli.SetEventHandler(NewEventHandler(c))
		if err = c.getSyncTable(); err != nil {
			return err
		}
		if lastpos.Name == "" {
			pos, err := c.getCurrentBinlogPosition()
			if err != nil {
				return fmt.Errorf("get current binlog position failed: %w", err)
			}
			lastpos = pos
			// 全量同步
			if err = c.syncFullData(ctx, c.tables); err != nil {
				return err
			}
		}
		go func() {
			if c.cfg.MonitorInter > 0 {
				ticker := time.Tick(time.Duration(c.cfg.MonitorInter) * time.Second)
				for _ = range ticker {
					c.monitoring()
				}
			}
		}()
		c.logger.Info().Any("sync from postion", lastpos).Msgf("")

		if tryCnt > 10 {
			c.stream.Err <- fmt.Errorf("maximum retry attempts reached ")
			<-c.restart
			tryCnt = 0
			backoff = 1
		}

		if err = c.cli.RunFrom(lastpos); err != nil {
			c.logger.Info().Any("sync from postion", lastpos).Err(err).Msgf("")
			if errors.Is(err, context.Canceled) {

			} else {
				c.stream.Err <- err
				c.cli.Close()
				tryCnt += 1
				backoff *= 2
				time.Sleep(time.Second * backoff)
			}
		}
	}
	return
}

func (c *Canal) SyncFromPos(pos mysql.Position) error {
	ok, err := c.checkPositionAvaliable(pos)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("the specified position does not exist")
	}
	c.restart <- 1
	c.posSaver.Save(pos)
	//reset connection
	c.cli.Close()
	return nil
}
func (c *Canal) Close() {
	c.cli.Close()
}

// getSyncTable 获取符合正则匹配的表
func (c *Canal) getSyncTable() error {
	rows, err := c.db.Query("SELECT table_schema, table_name FROM information_schema.tables")
	if err != nil {
		return err
	}
	defer rows.Close()
	tables := map[string]string{}
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return err
		}
		tables[table] = schema
	}
	if err := rows.Err(); err != nil {
		return err
	}
	syncTabels := []string{}
	for _, includes := range c.cfg.IncludeTableRegex {
		regex, err := regexp.Compile(includes)
		if err != nil {
			return err
		}
		for table, schema := range tables {
			fullTableName := fmt.Sprintf("%s.%s", schema, table)
			if regex.MatchString(fullTableName) {
				table, err := c.cli.GetTable(schema, table)
				if err != nil {
					return fmt.Errorf("get table info error: %w", err)
				}

				c.tables[fullTableName] = table
				syncTabels = append(syncTabels, fullTableName)
			}
		}

	}
	c.logger.Info().Str(logs.Canal, c.cfg.Addr).Any("tables", syncTabels).Msg("to sync")
	return nil
}

func (c *Canal) getCurrentBinlogPosition() (mysql.Position, error) {

	res, err := c.db.Query("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to execute SHOW MASTER STATUS: %w", err)
	}
	defer res.Close()

	if !res.Next() {
		return mysql.Position{}, fmt.Errorf("empty result from SHOW MASTER STATUS")
	}

	var binlogFile string
	var binlogPos uint32
	var unused1, unused2, unused3 sql.RawBytes
	if err := res.Scan(&binlogFile, &binlogPos, &unused1, &unused2, &unused3); err != nil {
		return mysql.Position{}, fmt.Errorf("failed to scan SHOW MASTER STATUS result: %w", err)
	}

	c.logger.Info().Str(logs.Canal, c.cfg.Addr).Str("binlog_file", binlogFile).Uint32("binlog_pos", binlogPos).Msg("current binlog position")

	return mysql.Position{
		Name: binlogFile,
		Pos:  binlogPos,
	}, nil
}

func (c *Canal) checkPositionAvaliable(pos mysql.Position) (bool, error) {

	res, err := c.db.Query("SHOW BINARY LOGS")
	if err != nil {
		return false, fmt.Errorf("failed to execute SHOW BINARY LOGS: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var binlogFileName string
		var unusedCol interface{}

		if err := res.Scan(&binlogFileName, &unusedCol); err != nil {
			return false, fmt.Errorf("failed to scan SHOW BINARY LOGS result: %w", err)
		}

		if binlogFileName == pos.Name {
			return true, nil
		}
	}

	return false, nil
}

func (c *Canal) syncFullData(ctx context.Context, tables map[string]*schema.Table) error {
	// 全量同步时，堵塞增量同步，防止 binlog 事件丢失
	atomic.StoreInt32(&c.isIncremental, 0)
	defer func() {
		atomic.StoreInt32(&c.isIncremental, 1)
		c.incrementCond.Broadcast()
	}()

	for _, table := range tables {
		var offset int
		for {
			// 在每次分页前检查上下文是否已取消，确保可以及时响应取消请求
			if ctx.Err() != nil {
				return ctx.Err()
			}

			query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d OFFSET %d", table.Schema, table.Name, c.cfg.FullSyncPageSize, offset)
			rows, err := c.db.QueryContext(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to sync full data from %s: %w", table.Name, err)
			}
			defer rows.Close()

			var values [][]interface{}
			for rows.Next() {
				rowData, scanErr := ScanRow(rows)
				if scanErr != nil {
					return fmt.Errorf("failed to scan row for table %s: %w", table.Name, scanErr)
				}
				values = append(values, rowData)
			}
			if len(values) == 0 {
				break
			}

			if err = c.storeFullData(ctx, table, values); err != nil {
				return fmt.Errorf("failed to store full data for table %s: %w", table.Name, err)
			}

			c.logger.Info().
				Str("database", table.Schema).
				Str("table", table.Name).
				Str("sync query", query).
				Msg("Successfully synced a page of full data")

			offset += c.cfg.FullSyncPageSize
			time.Sleep(time.Second * 2) // 控制频率，避免对数据库造成过大压力
		}
	}

	return nil
}

func ScanRow(rows *sql.Rows) ([]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	return values, nil
}

func (c *Canal) storeFullData(ctx context.Context, table *schema.Table, values [][]interface{}) error {
	rows := make([][]interface{}, len(values))
	for ridx, row := range values {
		values := make([]interface{}, len(table.Columns))
		for idx, col := range table.Columns {
			value, err := convertValueByColumnType(col, row[idx])
			if err != nil {
				return fmt.Errorf("covert %s.%s column %s value %v failed %w", table.Schema, table.Name, col.Name, row[idx], err)
			}
			values[idx] = value
		}
		rows[ridx] = values
	}
	return c.process(ctx, table.Schema, table.Name, string(handler.InsertEvent), rows)
}

func (c *Canal) GetTable(schema, table string) (*schema.Table, error) {
	ret, ok := c.tables[fmt.Sprintf("%s.%s", schema, table)]
	if !ok {
		return c.cli.GetTable(schema, table)
	}
	return ret, nil
}

func (c *Canal) OnTableChanged(schema, table string) {
	delete(c.tables, fmt.Sprintf("%s.%s", schema, table))
}

func (c *Canal) OnEvent(ctx context.Context, schema, table string, action string, rows [][]interface{}) error {
	if atomic.LoadInt32(&c.isIncremental) == 0 {
		c.incrementCond.L.Lock()
		for atomic.LoadInt32(&c.isIncremental) == 0 {
			c.incrementCond.Wait()
		}
		c.incrementCond.L.Unlock()
	}
	return c.process(ctx, schema, table, action, rows)
}
func (c *Canal) process(ctx context.Context, schema, table string, action string, rows [][]interface{}) error {
	meta, err := c.GetTable(schema, table)
	if err != nil {
		return err
	}
	for _, rows := range slices.Chunk(rows, 10) {
		drows := make([]map[string]interface{}, len(rows))
		for idx, row := range rows {
			ret := make(map[string]interface{})
			for idx, col := range meta.Columns {
				ret[col.Name] = row[idx]
			}
			drows[idx] = ret
		}
		data := map[string]interface{}{
			"action": action,
			"rows":   drows,
			"table":  fmt.Sprintf("%s.%s", schema, table),
		}
		event := stream.Event{Context: ctx, Topic: c.cfg.Addr, Datas: []map[string]interface{}{data}}
		c.stream.In <- event
		result := <-c.stream.Out
		if result.Error != nil {
			c.logger.Err(err).Any(logs.Input, "Canal").Any("event", event).Msg("process error")
			return err
		}
	}
	return nil
}

func (c *Canal) savePos(pos mysql.Position) error {
	c.logger.Info().
		Str(logs.Canal, c.cfg.Addr).
		Any("pos", fmt.Sprintf("%s.%d", pos.Name, pos.Pos)).Msg("save pos")
	err := c.posSaver.Save(pos)
	if err != nil {
		c.logger.Error().Err(err).Msg("save pos failed")
	}
	return err
}

func (c *Canal) ResyncTables(ctx context.Context, tables []string) (bool, error) {
	syncTables := map[string]*schema.Table{}
	for _, table := range tables {
		if syncTable, ok := c.tables[table]; ok {
			syncTables[table] = syncTable
		}
	}
	if len(syncTables) > 0 {
		go c.syncFullData(ctx, syncTables)
		return true, nil
	}
	return false, nil
}

func (c *Canal) ResyncTablesRegx(ctx context.Context, tables []string) (bool, error) {
	syncTables := map[string]*schema.Table{}
	for _, tableRegx := range tables {
		regex, err := regexp.Compile(tableRegx)
		if err != nil {
			return false, err
		}
		for fullName, syncTable := range c.tables {
			if regex.MatchString(fullName) {
				syncTables[fullName] = syncTable
			}
		}
	}
	if len(syncTables) > 0 {
		go c.syncFullData(ctx, syncTables)
		return true, nil
	}
	return false, nil
}

var _posfchanged = 0

func (c *Canal) monitoring() {
	pos, err := c.posSaver.Get()
	if err != nil {
		c.stream.Err <- fmt.Errorf("canal monitoring get cache pos failed:%s", err)
	}
	nowPos, err := c.getCurrentBinlogPosition()
	if err != nil {
		c.stream.Err <- fmt.Errorf("canal monitoring get now pos failed:%s", err)
	}
	if pos.Name != nowPos.Name {
		if _posfchanged > 5 {
			c.stream.Err <- fmt.Errorf("canal monitoring sync delay,  cached pos(%v) now pos(%v)", pos, nowPos)
		}
		_posfchanged += 1
		return
	}
	_posfchanged = 0
	delayBytes := nowPos.Pos - pos.Pos
	if delayBytes > uint32(c.cfg.DelayPos) {
		c.stream.Err <- fmt.Errorf("canal monitoring sync delay,  cached pos(%v) now pos(%v)", pos, nowPos)
	}
}
