// Package algorithm 实现统计器的实时计算、周期结算和断电后历史补算调度。
package algorithm

import (
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"NewStatis6.0/services"
	"NewStatis6.0/utils"
	"fmt"
	"sort"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

// 调度状态分为活跃配置快照、单统计器互斥锁和去重后的历史补算队列。
// 配置快照允许查询批处理无锁遍历，单统计器锁用于协调计算与配置更新、删除。
var (
	AccumulateMap = make(map[string]model.Accumulate)
	accTicker     *time.Ticker
	mapMu         sync.RWMutex
	accWake       = make(chan struct{}, 1)

	accumulateLocks sync.Map
	backfillOnce    sync.Once
	backfillQueue   = make(chan string, 20000)
	backfillQueued  sync.Map
)

// accumulateItem 将统计器配置与当前计算状态绑定，供一轮批处理使用。
type accumulateItem struct {
	Acc    model.Accumulate
	Record model.AccStatsValue
	New    bool
}

// stateUpdate 表示一条等待写入 MySQL 的统计游标更新。
type stateUpdate struct {
	Acc    model.Accumulate
	Record model.AccStatsValue
}

// pointRangeKey 将同一网关、周期和查询范围的统计器合并为一次批量查询。
type pointRangeKey struct {
	GatewayID   string
	PeriodStart int64
	Start       int64
	End         int64
}

// historyUpdate 描述一个已闭合周期的历史点及其是否需要写入。
type historyUpdate struct {
	Acc    model.Accumulate
	Period utils.Period
	Value  float64
	Write  bool
}

// closedUpdate 将历史结果和对应的下一周期状态作为一个持久化单元。
type closedUpdate struct {
	State   stateUpdate
	History historyUpdate
}

// GetAllAccumulate 从 MySQL 重新加载全部统计器，并在加载成功后原子替换内存快照。
func GetAllAccumulate() {
	data, err := services.LoadAllAccumulate()
	if err != nil {
		logger.Error("load accumulate config failed", err)
		return
	}
	next := make(map[string]model.Accumulate, len(data))
	for _, v := range data {
		next[v.ID] = v
	}
	mapMu.Lock()
	AccumulateMap = next
	mapMu.Unlock()
}

// StartAllAccumulate 启动单线程定时调度和低并发历史补算工作器。
// 定时触发与主动唤醒在同一协程执行，因此不会产生重叠的全量批处理轮次。
func StartAllAccumulate() {
	GetAllAccumulate()
	startBackfillWorkers()
	if accTicker != nil {
		accTicker.Stop()
	}
	interval := time.Duration(config.Conf.Stats.IntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	accTicker = time.NewTicker(interval)
	go func() {
		runTask := func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("accumulate task panic", fmt.Errorf("%v", r))
				}
			}()
			runOnce()
		}
		if config.Conf.Stats.CatchUpOnStart {
			runTask()
		}
		for {
			select {
			case <-accTicker.C:
				runTask()
			case <-accWake:
				runTask()
			}
		}
	}()
}

// NotifyAccumulates 非阻塞请求立即执行一轮统计；连续通知会被合并。
func NotifyAccumulates() {
	select {
	case accWake <- struct{}{}:
	default:
	}
}

// SuspendAccumulates 先从活跃快照移除统计器，再等待其在途计算释放互斥锁。
// ID 排序和逐个等待保证配置更新、删除与统计写回之间的确定性。
func SuspendAccumulates(ids []string) {
	unique := make(map[string]bool, len(ids))
	ordered := make([]string, 0, len(ids))
	mapMu.Lock()
	for _, id := range ids {
		if id == "" || unique[id] {
			continue
		}
		unique[id] = true
		ordered = append(ordered, id)
		delete(AccumulateMap, id)
	}
	mapMu.Unlock()
	sort.Strings(ordered)
	for _, id := range ordered {
		lock := accumulateLock(id)
		lock.Lock()
		lock.Unlock()
	}
}

// RunAccumulatesNow 按配置并发上限立即逐个处理指定统计器。
// 常规定时任务使用网关批处理，此入口用于保留独立触发的兼容能力。
func RunAccumulatesNow(list []model.Accumulate) {
	if len(list) == 0 {
		return
	}
	limit := config.Conf.Stats.MaxConcurrency
	if limit <= 0 {
		limit = 20
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for _, acc := range list {
		sem <- struct{}{}
		wg.Add(1)
		go func(item model.Accumulate) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := runOneAccumulate(item, utils.CurrentTime()); err != nil {
				logger.Error("handle accumulate failed", err)
			}
		}(acc)
	}
	wg.Wait()
}

// runOnce 执行一次全量调度：批量更新当前周期、结算刚结束周期，
// 并把落后一个以上周期的统计器交给后台补算队列。
func runOnce() {
	list := snapshotAccumulates()
	if len(list) == 0 {
		return
	}
	ids := make([]string, 0, len(list))
	for _, acc := range list {
		ids = append(ids, acc.ID)
	}
	recordMap, err := services.LoadAccStatsValueMap(ids)
	if err != nil {
		logger.Error("load accumulate states failed", err)
		return
	}

	// 按状态与当前时间的关系分类，避免深历史补算阻塞实时统计。
	now := utils.CurrentTime()
	currentItems := make([]accumulateItem, 0, len(list))
	closedItems := make([]accumulateItem, 0)
	backfillItems := make([]model.Accumulate, 0)
	settleDelay := int64(config.Conf.Stats.SettleDelaySeconds)
	for _, acc := range list {
		record, exists := recordMap[acc.ID]
		if !exists || record.ID == "" {
			record = newAccumulateRecord(acc, now)
			if now < record.PeriodEndTime {
				currentItems = append(currentItems, accumulateItem{Acc: acc, Record: record, New: true})
			} else {
				backfillItems = append(backfillItems, acc)
			}
			continue
		}
		if now < record.PeriodEndTime {
			currentItems = append(currentItems, accumulateItem{Acc: acc, Record: record})
			continue
		}
		if now < record.PeriodEndTime+settleDelay {
			continue
		}
		period := utils.Period{Type: acc.Type, Start: record.PeriodStartTime, End: record.PeriodEndTime}
		next := utils.NextPeriod(period)
		if now < next.End {
			closedItems = append(closedItems, accumulateItem{Acc: acc, Record: record})
			continue
		}
		backfillItems = append(backfillItems, acc)
	}

	// 已闭合周期先落历史并推进状态，再读取新周期的实时采集值。
	runCurrentPeriodBatch(currentItems, now)
	advanced := runClosedPeriodBatch(closedItems)
	runCurrentPeriodBatch(advanced, now)
	for _, acc := range backfillItems {
		enqueueBackfill(acc.ID)
	}
}

// runCurrentPeriodBatch 按网关和时间范围批量读取当前周期首末采集值并更新状态。
// InfluxDB 查询失败时整组保持原状态；成功但无数据时不伪造采集边界值。
func runCurrentPeriodBatch(items []accumulateItem, now int64) {
	groups := make(map[pointRangeKey][]accumulateItem)
	for _, item := range items {
		period := utils.Period{Type: item.Record.Type, Start: item.Record.PeriodStartTime, End: item.Record.PeriodEndTime}
		start := effectiveQueryStart(item.Acc, period)
		end := now + 1
		if end > period.End {
			end = period.End
		}
		if start >= end {
			if item.New {
				saveStateUpdates([]stateUpdate{{Acc: item.Acc, Record: item.Record}})
			}
			continue
		}
		key := pointRangeKey{GatewayID: item.Acc.GatewayID, PeriodStart: period.Start, Start: start, End: end}
		groups[key] = append(groups[key], item)
	}
	for key, group := range groups {
		accList := make([]model.Accumulate, 0, len(group))
		for _, item := range group {
			accList = append(accList, item.Acc)
		}
		firstPoints, lastPoints, err := services.FindBoundaryPointsInRange(accList, key.Start, key.End)
		if err != nil {
			logger.Error("batch current point query failed", fmt.Errorf("gateway=%s start=%d end=%d: %w", key.GatewayID, key.Start, key.End, err))
			continue
		}
		updates := make([]stateUpdate, 0, len(group))
		for _, item := range group {
			firstPoint, okFirst := firstPoints[item.Acc.Point]
			lastPoint, okLast := lastPoints[item.Acc.Point]
			if okFirst != okLast {
				logger.Error("batch current point result mismatch", fmt.Errorf("stats id=%s", item.Acc.ID))
				continue
			}
			if !okFirst {
				if item.New {
					updates = append(updates, stateUpdate{Acc: item.Acc, Record: item.Record})
				}
				continue
			}
			updated := applyCurrentPoints(item.Record, firstPoint, lastPoint)
			if item.New || updated != item.Record {
				updates = append(updates, stateUpdate{Acc: item.Acc, Record: updated})
			}
		}
		saveStateUpdates(updates)
	}
}

// runClosedPeriodBatch 批量结算只落后一个周期的统计器，并返回已推进到当前周期的状态。
// 已存在的历史点不会覆盖，无数据周期是否写 0 由配置决定。
func runClosedPeriodBatch(items []accumulateItem) []accumulateItem {
	groups := make(map[pointRangeKey][]accumulateItem)
	for _, item := range items {
		period := utils.Period{Type: item.Acc.Type, Start: item.Record.PeriodStartTime, End: item.Record.PeriodEndTime}
		key := pointRangeKey{GatewayID: item.Acc.GatewayID, PeriodStart: period.Start, Start: effectiveQueryStart(item.Acc, period), End: period.End}
		groups[key] = append(groups[key], item)
	}
	var advanced []accumulateItem
	for key, group := range groups {
		accList := make([]model.Accumulate, 0, len(group))
		for _, item := range group {
			accList = append(accList, item.Acc)
		}
		firstPoints, lastPoints, err := services.FindBoundaryPointsInRange(accList, key.Start, key.End)
		if err != nil {
			logger.Error("batch closed point query failed", fmt.Errorf("gateway=%s start=%d end=%d: %w", key.GatewayID, key.Start, key.End, err))
			continue
		}
		statsIDs := make([]string, 0, len(group))
		for _, item := range group {
			statsIDs = append(statsIDs, item.Acc.ID)
		}
		existingHistory, err := services.FindExistingHistoryIDsAtTime(statsIDs, key.PeriodStart)
		if err != nil {
			logger.Error("batch existing history query failed", fmt.Errorf("period=%d: %w", key.PeriodStart, err))
			continue
		}
		updates := make([]closedUpdate, 0, len(group))
		for _, item := range group {
			firstPoint, okFirst := firstPoints[item.Acc.Point]
			lastPoint, okLast := lastPoints[item.Acc.Point]
			if okFirst != okLast {
				logger.Error("batch closed point result mismatch", fmt.Errorf("stats id=%s", item.Acc.ID))
				continue
			}
			nextRecord, value := closePeriodRecord(item.Acc, item.Record, firstPoint, lastPoint, okFirst)
			updates = append(updates, closedUpdate{
				State: stateUpdate{Acc: item.Acc, Record: nextRecord},
				History: historyUpdate{
					Acc:    item.Acc,
					Period: utils.Period{Type: item.Acc.Type, Start: item.Record.PeriodStartTime, End: item.Record.PeriodEndTime},
					Value:  value,
					Write:  !existingHistory[item.Acc.ID] && (okFirst || config.Conf.Stats.WriteZeroWhenPeriodNoData),
				},
			})
		}
		for _, saved := range persistClosedUpdates(updates) {
			advanced = append(advanced, accumulateItem{Acc: saved.Acc, Record: saved.Record})
		}
	}
	return advanced
}

// applyCurrentPoints 更新当前周期采集边界。
// 连续周期保留上周期末值作为起点；断档恢复后改用本周期首个有效点重新起算。
func applyCurrentPoints(record model.AccStatsValue, firstPoint, lastPoint services.PointValue) model.AccStatsValue {
	if record.Gap || record.StartTime == 0 {
		record.StartTime = firstPoint.Time
		record.StartValue = firstPoint.Value
		record.EndTime = lastPoint.Time
		record.EndValue = lastPoint.Value
		record.HasData = true
		record.Gap = false
		return record
	}
	record.EndTime = lastPoint.Time
	record.EndValue = lastPoint.Value
	record.HasData = true
	return record
}

// closePeriodRecord 计算闭合周期增量并构造下一周期状态。
// 无数据会输出 0 并标记断档；恢复周期从首个有效点开始，只有一个点时增量为 0。
func closePeriodRecord(acc model.Accumulate, record model.AccStatsValue, firstPoint, lastPoint services.PointValue, hasData bool) (model.AccStatsValue, float64) {
	period := utils.Period{Type: acc.Type, Start: record.PeriodStartTime, End: record.PeriodEndTime}
	startPoint := services.PointValue{Time: record.StartTime, Value: record.StartValue}
	hasStart := record.HasData && !record.Gap && record.StartTime != 0
	gap := record.Gap || !record.HasData
	value := float64(0)
	if !hasData {
		gap = true
		hasStart = false
	} else {
		if gap || !hasStart {
			startPoint = firstPoint
		}
		value = lastPoint.Value - startPoint.Value
		if value < 0 && config.Conf.Stats.NegativeDeltaPolicy == "zero" {
			logger.Warn("negative delta converted to zero", acc.ID)
			value = 0
		}
		if lastPoint.Time <= startPoint.Time {
			value = 0
		}
		startPoint = lastPoint
		gap = false
		hasStart = true
	}
	next := utils.NextPeriod(period)
	return model.AccStatsValue{
		ID:              acc.ID,
		Type:            acc.Type,
		PeriodStartTime: next.Start,
		PeriodEndTime:   next.End,
		StartTime:       startPoint.Time,
		StartValue:      startPoint.Value,
		EndTime:         startPoint.Time,
		EndValue:        startPoint.Value,
		HasData:         hasStart && !gap,
		Gap:             gap,
	}, value
}

// writeHistoryUpdates 按配置拆批写入需要新增的周期历史点。
func writeHistoryUpdates(updates []historyUpdate) error {
	batchSize := config.Conf.Stats.HistoryWriteBatchSize
	if batchSize <= 0 {
		batchSize = 2000
	}
	for offset := 0; offset < len(updates); offset += batchSize {
		limit := offset + batchSize
		if limit > len(updates) {
			limit = len(updates)
		}
		bp, err := newBatchPoints()
		if err != nil {
			return err
		}
		for _, update := range updates[offset:limit] {
			if !update.Write {
				continue
			}
			if err = services.WriteHistoryPoint(update.Acc, update.Period, update.Value, bp); err != nil {
				return err
			}
		}
		if len(bp.Points()) > 0 {
			if err = common.GetInfluxDB().Write(bp); err != nil {
				return err
			}
		}
	}
	return nil
}

// persistClosedUpdates 在单统计器锁保护下先写 InfluxDB 历史，再推进 MySQL 状态。
// 该顺序保证历史写失败时游标不前进，服务恢复后仍会重试当前周期。
func persistClosedUpdates(updates []closedUpdate) []stateUpdate {
	if len(updates) == 0 {
		return nil
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].State.Acc.ID < updates[j].State.Acc.ID })
	batchSize := config.Conf.Stats.StateUpsertBatchSize
	if historyBatchSize := config.Conf.Stats.HistoryWriteBatchSize; historyBatchSize > 0 && (batchSize <= 0 || historyBatchSize < batchSize) {
		batchSize = historyBatchSize
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	var saved []stateUpdate
	for offset := 0; offset < len(updates); offset += batchSize {
		limit := offset + batchSize
		if limit > len(updates) {
			limit = len(updates)
		}
		chunk := updates[offset:limit]
		locks := make([]*sync.Mutex, 0, len(chunk))
		for _, update := range chunk {
			lock := accumulateLock(update.State.Acc.ID)
			lock.Lock()
			locks = append(locks, lock)
		}
		activeStates := make([]stateUpdate, 0, len(chunk))
		activeHistories := make([]historyUpdate, 0, len(chunk))
		values := make([]model.AccStatsValue, 0, len(chunk))
		for _, update := range chunk {
			if !isAccumulateActive(update.State.Acc) {
				continue
			}
			activeStates = append(activeStates, update.State)
			activeHistories = append(activeHistories, update.History)
			values = append(values, update.State.Record)
		}
		err := writeHistoryUpdates(activeHistories)
		if err == nil {
			err = services.SaveAccStatsValues(values)
		}
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
		if err != nil {
			logger.Error("persist closed accumulate states failed", err)
			continue
		}
		saved = append(saved, activeStates...)
	}
	return saved
}

// saveStateUpdates 分批保存当前周期状态，并过滤已删除或已变更配置产生的旧结果。
func saveStateUpdates(updates []stateUpdate) []stateUpdate {
	if len(updates) == 0 {
		return nil
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].Acc.ID < updates[j].Acc.ID })
	batchSize := config.Conf.Stats.StateUpsertBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	var saved []stateUpdate
	for offset := 0; offset < len(updates); offset += batchSize {
		limit := offset + batchSize
		if limit > len(updates) {
			limit = len(updates)
		}
		chunk := updates[offset:limit]
		locks := make([]*sync.Mutex, 0, len(chunk))
		for _, update := range chunk {
			lock := accumulateLock(update.Acc.ID)
			lock.Lock()
			locks = append(locks, lock)
		}
		active := make([]stateUpdate, 0, len(chunk))
		values := make([]model.AccStatsValue, 0, len(chunk))
		for _, update := range chunk {
			if isAccumulateActive(update.Acc) {
				active = append(active, update)
				values = append(values, update.Record)
			}
		}
		err := services.SaveAccStatsValues(values)
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
		if err != nil {
			logger.Error("batch save accumulate states failed", err)
			continue
		}
		saved = append(saved, active...)
	}
	return saved
}

// startBackfillWorkers 按启动时配置创建固定数量的历史补算工作器。
func startBackfillWorkers() {
	backfillOnce.Do(func() {
		workerCount := config.Conf.Stats.BackfillConcurrency
		if workerCount <= 0 {
			workerCount = 3
		}
		for i := 0; i < workerCount; i++ {
			go backfillWorker()
		}
	})
}

// enqueueBackfill 将统计器加入补算队列，并保证同一 ID 最多只有一个排队任务。
func enqueueBackfill(id string) {
	if id == "" {
		return
	}
	if _, loaded := backfillQueued.LoadOrStore(id, true); loaded {
		return
	}
	select {
	case backfillQueue <- id:
	default:
		backfillQueued.Delete(id)
		logger.Error("backfill queue is full", fmt.Errorf("stats id=%s", id))
	}
}

// backfillWorker 分批推进单个统计器的历史游标，失败后延迟重试，
// 尚未追到实时周期时重新排队，以便多个统计器公平轮转。
func backfillWorker() {
	for id := range backfillQueue {
		acc, active := getActiveAccumulate(id)
		var err error
		if active {
			err = runOneAccumulate(acc, utils.CurrentTime())
			if err != nil {
				logger.Error("backfill accumulate failed", err)
			}
		}
		backfillQueued.Delete(id)
		if !active {
			continue
		}
		if err != nil {
			time.Sleep(time.Second)
			enqueueBackfill(id)
			continue
		}
		if accumulateNeedsBackfill(id, utils.CurrentTime()) {
			enqueueBackfill(id)
		}
	}
}

// accumulateNeedsBackfill 判断状态是否仍落后于允许结算的最近周期。
func accumulateNeedsBackfill(id string, now int64) bool {
	record, err := services.LoadAccStatsValue(id)
	if err != nil {
		logger.Error("load backfill state failed", err)
		return true
	}
	if record.ID == "" {
		return true
	}
	return now >= record.PeriodEndTime+int64(config.Conf.Stats.SettleDelaySeconds)
}

// runOneAccumulate 串行处理一个统计器，并拒绝已经停用或被新配置替代的快照。
func runOneAccumulate(acc model.Accumulate, now int64) error {
	lock := accumulateLock(acc.ID)
	lock.Lock()
	defer lock.Unlock()
	if !isAccumulateActive(acc) {
		return nil
	}
	return HandleAccumulate(acc, now)
}

// accumulateLock 返回统计器 ID 对应的进程内互斥锁。
func accumulateLock(id string) *sync.Mutex {
	lock, _ := accumulateLocks.LoadOrStore(id, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// snapshotAccumulates 复制当前活跃统计器，使后续查询无需长期持有读锁。
func snapshotAccumulates() []model.Accumulate {
	mapMu.RLock()
	list := make([]model.Accumulate, 0, len(AccumulateMap))
	for _, acc := range AccumulateMap {
		list = append(list, acc)
	}
	mapMu.RUnlock()
	return list
}

// getActiveAccumulate 从内存快照读取仍然活跃的统计器配置。
func getActiveAccumulate(id string) (model.Accumulate, bool) {
	mapMu.RLock()
	acc, ok := AccumulateMap[id]
	mapMu.RUnlock()
	return acc, ok
}

// isAccumulateActive 同时校验 ID 存在且完整配置未变化，防止旧任务写回新配置状态。
func isAccumulateActive(acc model.Accumulate) bool {
	current, ok := getActiveAccumulate(acc.ID)
	return ok && current == acc
}

// HandleAccumulate 执行单统计器兼容计算流程：初始化、更新实时周期或分批补算。
// 补算历史点成功写入 InfluxDB 后才保存 MySQL 游标，确保错误可安全重试。
func HandleAccumulate(acc model.Accumulate, now int64) error {
	bp, err := newBatchPoints()
	if err != nil {
		return err
	}
	record, err := services.LoadAccStatsValue(acc.ID)
	if err != nil {
		return err
	}
	if record.ID == "" {
		record, err = initAccumulateRecord(acc, now, bp)
	} else if now < record.PeriodEndTime {
		record, err = updateCurrentPeriodRecord(acc, record, now)
	} else {
		record, err = catchUpClosedPeriods(acc, record, now, bp)
		if err == nil && now < record.PeriodEndTime {
			record, err = updateCurrentPeriodRecord(acc, record, now)
		}
	}
	if err != nil {
		return err
	}
	if len(bp.Points()) > 0 {
		if err = common.GetInfluxDB().Write(bp); err != nil {
			return err
		}
	}
	return services.SaveAccStatsValue(record)
}

// newBatchPoints 使用配置中的数据库和时间精度创建 InfluxDB 批量写入容器。
func newBatchPoints() (client.BatchPoints, error) {
	return client.NewBatchPoints(client.BatchPointsConfig{
		Database:  config.Conf.Influx.Database,
		Precision: config.Conf.Influx.Precision,
	})
}

// newAccumulateRecord 以统计器配置的开始时间建立初始周期。
// 开始时间无效时退回当前时间；初始状态标记为断档，等待首个有效采集点。
func newAccumulateRecord(acc model.Accumulate, now int64) model.AccStatsValue {
	baseTime := utils.Timestamp(acc.StartTime)
	if baseTime == 0 {
		baseTime = now
	}
	period := utils.BuildPeriod(acc.Type, baseTime)
	return model.AccStatsValue{
		ID:              acc.ID,
		Type:            acc.Type,
		PeriodStartTime: period.Start,
		PeriodEndTime:   period.End,
		Gap:             true,
		HasData:         false,
	}
}

// initAccumulateRecord 补算配置开始时间以来的闭合周期，并在追平后更新当前周期。
func initAccumulateRecord(acc model.Accumulate, now int64, bp client.BatchPoints) (model.AccStatsValue, error) {
	record := newAccumulateRecord(acc, now)
	var err error
	if record.PeriodEndTime <= now {
		record, err = catchUpClosedPeriods(acc, record, now, bp)
		if err != nil {
			return record, err
		}
	}
	if now < record.PeriodEndTime {
		return updateCurrentPeriodRecord(acc, record, now)
	}
	return record, nil
}

// updateCurrentPeriodRecord 查询当前周期截至 now 的首末有效点并更新实时状态。
// 查询成功但无采集点时保持状态不变，查询错误则返回上层重试。
func updateCurrentPeriodRecord(acc model.Accumulate, record model.AccStatsValue, now int64) (model.AccStatsValue, error) {
	period := utils.Period{Type: record.Type, Start: record.PeriodStartTime, End: record.PeriodEndTime}
	queryStart := effectiveQueryStart(acc, period)
	queryEnd := now + 1
	if queryEnd > period.End {
		queryEnd = period.End
	}
	firstPoint, okFirst, err := services.FindFirstPointInRange(acc, queryStart, queryEnd)
	if err != nil {
		return record, err
	}
	lastPoint, okLast, err := services.FindLastPointInRange(acc, queryStart, queryEnd)
	if err != nil {
		return record, err
	}
	if !okFirst && !okLast {
		return record, nil
	}
	if okFirst != okLast {
		return record, fmt.Errorf("current point result mismatch for stats id=%s", acc.ID)
	}
	return applyCurrentPoints(record, firstPoint, lastPoint), nil
}

// catchUpClosedPeriods 从持久化游标开始，按配置上限补算已经结束且超过结算延迟的周期。
// 成功查询但整周期无数据时可写 0 并建立断档；InfluxDB 错误时不写 0、不推进游标。
// 已存在历史点保持原值不覆盖，适用于生产数据和断电重启后的幂等恢复。
func catchUpClosedPeriods(acc model.Accumulate, record model.AccStatsValue, now int64, bp client.BatchPoints) (model.AccStatsValue, error) {
	period := utils.Period{Type: acc.Type, Start: record.PeriodStartTime, End: record.PeriodEndTime}
	historyEnd := catchUpHistoryEnd(period, now)
	existingHistory, err := services.FindHistoryPointTimes(acc.ID, period.Start, historyEnd)
	if err != nil {
		return record, err
	}
	// 连续周期沿用上一周期末点；遇到空周期后清除基线，恢复时从新首点起算。
	startPoint := services.PointValue{Time: record.StartTime, Value: record.StartValue}
	hasStart := record.HasData && !record.Gap && record.StartTime != 0
	gap := record.Gap || !record.HasData
	processed := 0
	for period.End <= now && processed < config.Conf.Stats.CatchUpBatchSize {
		if now < period.End+int64(config.Conf.Stats.SettleDelaySeconds) {
			break
		}
		queryStart := effectiveQueryStart(acc, period)
		firstPoint, okFirst, err := services.FindFirstPointInRange(acc, queryStart, period.End)
		if err != nil {
			return record, err
		}
		lastPoint, okLast, err := services.FindLastPointInRange(acc, queryStart, period.End)
		if err != nil {
			return record, err
		}
		if okFirst != okLast {
			return record, fmt.Errorf("closed point result mismatch for stats id=%s", acc.ID)
		}
		if !okFirst {
			if config.Conf.Stats.WriteZeroWhenPeriodNoData && !existingHistory[period.Start] {
				if err = services.WriteHistoryPoint(acc, period, 0, bp); err != nil {
					return record, err
				}
			}
			gap = true
			hasStart = false
			period = utils.NextPeriod(period)
			processed++
			continue
		}
		if gap || !hasStart {
			startPoint = firstPoint
			hasStart = true
		}
		value := lastPoint.Value - startPoint.Value
		if value < 0 && config.Conf.Stats.NegativeDeltaPolicy == "zero" {
			logger.Warn("negative delta converted to zero", acc.ID)
			value = 0
		}
		if lastPoint.Time <= startPoint.Time {
			value = 0
		}
		if !existingHistory[period.Start] {
			if err = services.WriteHistoryPoint(acc, period, value, bp); err != nil {
				return record, err
			}
		}
		startPoint = lastPoint
		gap = false
		period = utils.NextPeriod(period)
		processed++
	}
	return model.AccStatsValue{
		ID:              acc.ID,
		Type:            acc.Type,
		PeriodStartTime: period.Start,
		PeriodEndTime:   period.End,
		StartTime:       startPoint.Time,
		StartValue:      startPoint.Value,
		EndTime:         startPoint.Time,
		EndValue:        startPoint.Value,
		HasData:         hasStart && !gap,
		Gap:             gap,
	}, nil
}

// catchUpHistoryEnd 计算本批补算可能覆盖的历史时间上界，用于一次性读取已有点。
func catchUpHistoryEnd(period utils.Period, now int64) int64 {
	end := period.Start
	current := period
	batchSize := config.Conf.Stats.CatchUpBatchSize
	if batchSize <= 0 {
		batchSize = 200
	}
	for processed := 0; current.End <= now && processed < batchSize; processed++ {
		if now < current.End+int64(config.Conf.Stats.SettleDelaySeconds) {
			break
		}
		end = current.End
		current = utils.NextPeriod(current)
	}
	return end
}

// effectiveQueryStart 保证统计器首个周期不会读取配置开始时间之前的采集数据。
func effectiveQueryStart(acc model.Accumulate, period utils.Period) int64 {
	startTime := utils.Timestamp(acc.StartTime)
	if startTime > period.Start && startTime < period.End {
		return startTime
	}
	return period.Start
}
