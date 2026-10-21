package algorithm

import (
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"NewStatis6.0/services"
	"NewStatis6.0/utils"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

// calibrationClient 每次查询前检查轮次预算；在途查询由独立 HTTP 超时兜底。
type calibrationClient struct {
	client.Client
	ctx context.Context
}

func (c calibrationClient) Query(q client.Query) (*client.Response, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	res, err := c.Client.Query(q)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("nil calibration query response")
	}
	if err = res.Error(); err != nil {
		return nil, err
	}
	if len(res.Results) != 1 {
		return nil, fmt.Errorf("unexpected calibration query result count")
	}
	for _, series := range res.Results[0].Series {
		if series.Tags["id"] == "" {
			return nil, fmt.Errorf("calibration response has no series id")
		}
	}
	return res, nil
}

type calibrationRunner struct {
	cfg        config.CalibrationConfig
	store      services.CalibrationStore
	conn       client.Client
	mu         sync.Mutex
	errors     int
	pauseUntil int64
}

// StartCalibration 启动独立审计循环；关闭或初始化失败不会影响原统计服务。
// 返回函数取消后等待在途查询完成，最长受审计客户端超时限制。
func StartCalibration() func() {
	cfg := config.Conf.Calibration
	if !cfg.Enable {
		return func() {}
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("calibration disabled", err)
		return func() {}
	}
	if config.Conf.MySQL.MaxOpenConns == 1 {
		logger.Error("calibration disabled", fmt.Errorf("mysql.max_open_conns must allow a lock connection and a work connection"))
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &calibrationRunner{cfg: cfg, store: services.CalibrationStore{DB: common.GetMysql(), Timeout: time.Duration(cfg.WriteTimeoutSeconds) * time.Second}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.store.Init(ctx); err != nil {
			logger.Error("calibration initialization failed; normal statistics continue", err)
			return
		}
		ic := config.Conf.Influx
		var err error
		r.conn, err = client.NewHTTPClient(client.HTTPConfig{Addr: fmt.Sprintf("http://%s:%s", ic.Host, ic.Port), Username: ic.Username, Password: ic.Password, Timeout: time.Duration(cfg.QueryTimeoutSeconds) * time.Second})
		if err != nil {
			logger.Error("calibration client initialization failed", err)
			return
		}
		defer r.conn.Close()
		logger.Info("calibration audit started", map[string]interface{}{"concurrency": cfg.Concurrency, "batch_size": cfg.StatsBatchSize})
		// 重试检查与新配置扫描分开计时，失败等待不会强制再等待一次扫描间隔。
		ticker := time.NewTicker(time.Duration(cfg.RetryIntervalSeconds) * time.Second)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			if err = r.round(ctx); err != nil && ctx.Err() == nil {
				logger.Error("calibration round failed", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}

// sourceHash 包含会影响计算的配置和数据源；换源或改算法策略后旧任务标记失效。
func calibrationSourceHash(a model.Accumulate) string {
	b, _ := json.Marshal(struct {
		Acc                                                 model.Accumulate
		Database, Measurement, Policy, Timezone, Host, Port string
	}{a, config.Conf.Influx.Database, config.Conf.Influx.HistoryMeasurement, config.Conf.Stats.NegativeDeltaPolicy, config.Conf.Server.Timezone, config.Conf.Influx.Host, config.Conf.Influx.Port})
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

// calibrationWindow 选取最近 N 个已结束且已等待稳定期的自然周期，裁剪到配置起始周期。
func calibrationWindow(acc model.Accumulate, now int64, cfg config.CalibrationConfig) (int64, int64, error) {
	start := utils.Timestamp(acc.StartTime)
	if start <= 0 || acc.Type < 0 || acc.Type > 3 || acc.GatewayID == "" || acc.Point == "" {
		return 0, 0, fmt.Errorf("invalid statistic configuration: %s", acc.ID)
	}
	end := utils.BuildPeriod(acc.Type, now-int64(cfg.SettleDelaySeconds)).Start
	first := end
	for i := 0; i < cfg.RecentPeriods; i++ {
		first = utils.BuildPeriod(acc.Type, first-1).Start
	}
	configured := utils.BuildPeriod(acc.Type, start).Start
	if first < configured {
		first = configured
	}
	if first >= end {
		return 0, 0, nil
	}
	return first, end, nil
}

func (r *calibrationRunner) newTask(acc model.Accumulate, now int64) (*model.CalibrationTask, error) {
	start, end, err := calibrationWindow(acc, now, r.cfg)
	if err != nil {
		logger.Warn("calibration skipped invalid statistic", acc.ID)
		return nil, nil
	}
	if start == end {
		return nil, nil
	}
	b, err := json.Marshal(acc)
	if err != nil {
		return nil, err
	}
	return &model.CalibrationTask{StatsID: acc.ID, Snapshot: string(b), SourceHash: calibrationSourceHash(acc), Start: start, End: end, Status: "pending"}, nil
}

// round 用 MySQL 连接级命名锁防止多个实例重复执行及错误恢复其他实例的任务。
// 命名锁连接仅在本轮持有，全部业务数据库操作仍是短事务。
func (r *calibrationRunner) round(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, time.Duration(r.cfg.MaxTaskSeconds)*time.Second)
	defer cancel()
	db, err := r.store.DB.DB()
	if err != nil {
		return err
	}
	lockConn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer lockConn.Close()
	lockName := fmt.Sprintf("newstats_calibration_%x", sha256.Sum256([]byte(config.Conf.MySQL.Database)))[:60]
	var locked sql.NullInt64
	if err = lockConn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&locked); err != nil {
		return err
	}
	if !locked.Valid || locked.Int64 != 1 {
		return nil
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), time.Duration(r.cfg.WriteTimeoutSeconds)*time.Second)
		defer stop()
		var released sql.NullInt64
		if e := lockConn.QueryRowContext(c, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released); e != nil {
			logger.Error("calibration lock release failed", e)
			// 未确认释放时销毁物理连接，避免把仍持锁的会话放回连接池。
			_ = lockConn.Raw(func(interface{}) error { return driver.ErrBadConn })
		}
	}()
	cursor, err := r.store.Cursor(ctx)
	if err != nil {
		return err
	}
	r.errors, r.pauseUntil = cursor.ConsecutiveErrors, cursor.PauseUntil
	if time.Now().Unix() < r.pauseUntil {
		return nil
	}
	if err = r.store.Recover(ctx); err != nil {
		return err
	}
	now := time.Now().Unix()
	if cursor.NextScan <= now {
		if err = r.store.Scan(ctx, r.cfg.StatsBatchSize, now+int64(r.cfg.IntervalSeconds), func(a model.Accumulate) (*model.CalibrationTask, error) { return r.newTask(a, now) }); err != nil {
			return err
		}
	}
	tasks, err := r.store.Pending(ctx, now, r.cfg.StatsBatchSize)
	if err != nil {
		return err
	}
	groups := groupCalibrationTasks(tasks, r.cfg.PointBatchSize)
	jobs := make(chan []model.CalibrationTask)
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				r.mu.Lock()
				paused := time.Now().Unix() < r.pauseUntil
				r.mu.Unlock()
				if ctx.Err() != nil || paused {
					continue
				}
				err := r.process(ctx, batch)
				if ctx.Err() != nil {
					continue
				}
				r.mu.Lock()
				if err != nil {
					r.errors++
					if r.errors >= r.cfg.PauseAfterConsecutiveErrors {
						r.pauseUntil = time.Now().Unix() + int64(r.cfg.ErrorPauseSeconds)
					}
					logger.Error("calibration batch failed", err)
				} else {
					r.errors = 0
				}
				// 预算到期也保存故障暂停，使用短独立超时，不启动任何新的 Influx 查询。
				healthCtx, stop := context.WithTimeout(parent, time.Duration(r.cfg.WriteTimeoutSeconds)*time.Second)
				if e := r.store.SaveHealth(healthCtx, r.errors, r.pauseUntil); e != nil {
					logger.Error("calibration health save failed", e)
				}
				stop()
				r.mu.Unlock()
			}
		}()
	}
dispatch:
	for _, g := range groups {
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- g:
		}
	}
	close(jobs)
	wg.Wait()
	if ctx.Err() != nil {
		if parent.Err() != nil {
			return parent.Err()
		}
		logger.Info("calibration round budget reached; pending tasks will resume", nil)
		return nil
	}
	return r.store.Cleanup(ctx, time.Now().AddDate(0, 0, -r.cfg.AuditRetentionDays), r.cfg.StatsBatchSize)
}

// groupCalibrationTasks 仅合并网关、类型、周期及初始截断时间相同的任务。
func groupCalibrationTasks(tasks []model.CalibrationTask, limit int) [][]model.CalibrationTask {
	groups := map[string][]model.CalibrationTask{}
	keys := []string{}
	for _, t := range tasks {
		a, e := services.CalibrationSnapshot(t)
		key := fmt.Sprintf("invalid:%d", t.ID)
		if e == nil {
			cutoff := utils.Timestamp(a.StartTime)
			if cutoff < utils.BuildPeriod(a.Type, t.Start-1).Start {
				cutoff = 0
			}
			key = fmt.Sprintf("%s/%d/%d/%d/%d", a.GatewayID, a.Type, t.Start, t.End, cutoff)
		}
		if _, ok := groups[key]; !ok {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], t)
	}
	// 按最旧任务优先，同时让同范围查询能够批处理。
	sort.SliceStable(keys, func(i, j int) bool { return groups[keys[i]][0].ID < groups[keys[j]][0].ID })
	var result [][]model.CalibrationTask
	for _, key := range keys {
		list := groups[key]
		for len(list) > 0 {
			n := limit
			if n > len(list) {
				n = len(list)
			}
			result = append(result, list[:n])
			list = list[n:]
		}
	}
	return result
}

func (r *calibrationRunner) process(ctx context.Context, tasks []model.CalibrationTask) (err error) {
	if err = r.store.Claim(ctx, tasks); err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("calibration task panic: %v", p)
		}
		if err != nil {
			c, stop := context.WithTimeout(context.Background(), time.Duration(r.cfg.WriteTimeoutSeconds)*time.Second)
			defer stop()
			if ctx.Err() != nil {
				if e := r.store.Yield(c, tasks, time.Now().Unix()+int64(r.cfg.RetryIntervalSeconds)); e != nil {
					logger.Error("calibration yield state save failed", e)
				}
				return
			}
			if e := r.store.Fail(c, tasks, err, *r.cfg.RetryCount, time.Now().Unix()+int64(r.cfg.RetryIntervalSeconds)); e != nil {
				logger.Error("calibration failure state save failed", e)
			}
		}
	}()
	configs, err := r.store.Configs(ctx, tasks)
	if err != nil {
		return err
	}
	valid := make([]model.CalibrationTask, 0, len(tasks))
	for _, t := range tasks {
		a, ok := configs[t.StatsID]
		if !ok || calibrationSourceHash(a) != t.SourceHash {
			if err = r.store.Finish(ctx, t, nil, "stale"); err != nil {
				return err
			}
			continue
		}
		valid = append(valid, t)
	}
	if len(valid) == 0 {
		return nil
	}
	conn := calibrationClient{r.conn, ctx}
	details, err := auditCalibrationBatch(conn, valid)
	if err != nil {
		return err
	}
	// 查询期间可能发生配置修改或删除，再检查一次，避免把旧配置比较结果作为有效审计。
	configs, err = r.store.Configs(ctx, valid)
	if err != nil {
		return err
	}
	for _, t := range valid {
		a, ok := configs[t.StatsID]
		status := "done"
		rows := details[t.ID]
		if !ok || calibrationSourceHash(a) != t.SourceHash {
			status = "stale"
			rows = nil
		}
		if err = r.store.Finish(ctx, t, rows, status); err != nil {
			return err
		}
		for _, d := range rows {
			if d.Status != "match" {
				logger.Warn("calibration audit finding", map[string]interface{}{"task_id": t.ID, "stats_id": t.StatsID, "period_start": d.PeriodStart, "status": d.Status, "expected": d.Expected, "actual": d.Actual})
			}
		}
	}
	return nil
}

// auditCalibrationBatch 先读紧邻前周期作为锚点，再按正式结算算法逐周期重算。
// 锚点缺失时无法判定是否已过原始数据保留期，候选值只供参考，不宣布历史错误。
func auditCalibrationBatch(conn client.Client, tasks []model.CalibrationTask) (map[uint64][]model.CalibrationDetail, error) {
	result := make(map[uint64][]model.CalibrationDetail)
	if len(tasks) == 0 {
		return result, nil
	}
	accs := make([]model.Accumulate, len(tasks))
	ids := []string{}
	points := []string{}
	for i, t := range tasks {
		a, e := services.CalibrationSnapshot(t)
		if e != nil {
			return nil, e
		}
		accs[i] = a
		ids = append(ids, a.ID)
		points = append(points, a.Point)
	}
	firstTask := tasks[0]
	acc := accs[0]
	anchor := utils.BuildPeriod(acc.Type, firstTask.Start-1)
	configured := utils.BuildPeriod(acc.Type, utils.Timestamp(acc.StartTime)).Start
	if anchor.Start < configured {
		anchor = utils.BuildPeriod(acc.Type, firstTask.Start)
	}
	records := make([]model.AccStatsValue, len(tasks))
	uncertain := make([]bool, len(tasks))
	for i, a := range accs {
		records[i] = model.AccStatsValue{ID: a.ID, Type: a.Type, PeriodStartTime: anchor.Start, PeriodEndTime: anchor.End, Gap: true}
	}
	history, err := services.ReadCalibrationHistory(conn, ids, firstTask.Start, firstTask.End)
	if err != nil {
		return nil, err
	}
	for period := anchor; period.Start < firstTask.End; period = utils.NextPeriod(period) {
		queryStart := period.Start
		if st := utils.Timestamp(acc.StartTime); st > queryStart {
			queryStart = st
		}
		first, e := services.QueryBoundarySelector(conn, acc.GatewayID, points, queryStart, period.End, "first")
		if e != nil {
			return nil, e
		}
		last, e := services.QueryBoundarySelector(conn, acc.GatewayID, points, queryStart, period.End, "last")
		if e != nil {
			return nil, e
		}
		for i, a := range accs {
			f, fok := first[a.Point]
			l, lok := last[a.Point]
			if fok != lok {
				return nil, fmt.Errorf("inconsistent raw boundaries: %s", a.ID)
			}
			if fok && (f.Time < queryStart || l.Time >= period.End || f.Time > l.Time || math.IsNaN(f.Value) || math.IsNaN(l.Value) || math.IsInf(f.Value, 0) || math.IsInf(l.Value, 0)) {
				return nil, fmt.Errorf("invalid raw boundaries: %s", a.ID)
			}
			previous := records[i]
			next, value := closePeriodRecord(a, previous, f, l, fok)
			// 审计从原始累计值重算，边界不提前舍入，仅候选结果保留三位。
			records[i] = next
			if period.Start < firstTask.Start {
				uncertain[i] = !fok
				continue
			}
			d := model.CalibrationDetail{TaskID: tasks[i].ID, PeriodStart: period.Start, PeriodEnd: period.End, HasData: fok, Expected: utils.RoundStatsValue(value)}
			if fok {
				d.StartTime = f.Time
				d.StartValue = f.Value
				d.EndTime = l.Time
				d.EndValue = l.Value
				if previous.HasData && !previous.Gap {
					d.StartTime = previous.StartTime
					d.StartValue = previous.StartValue
				}
			}
			h, hExists := history[a.ID][period.Start]
			if hExists {
				v := utils.RoundStatsValue(h.Value)
				d.Actual = &v
			}
			d.Status = calibrationComparison(d, hExists, h.Type, a.Type, uncertain[i])
			result[tasks[i].ID] = append(result[tasks[i].ID], d)
			uncertain[i] = !fok
		}
	}
	return result, nil
}

func calibrationComparison(d model.CalibrationDetail, exists bool, historyType, typ int, uncertain bool) string {
	if !d.HasData || uncertain {
		return "unverifiable_raw"
	}
	if !exists {
		return "missing_history"
	}
	if historyType != typ {
		return "type_mismatch"
	}
	if d.Actual != nil && math.Abs(d.Expected-*d.Actual) < 0.0005 {
		return "match"
	}
	return "different"
}
