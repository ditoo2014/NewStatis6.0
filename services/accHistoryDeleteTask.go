package services

import (
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 历史删除任务状态用于持久化调度进度，使服务重启后可以继续执行。
const (
	AccHistoryDeletePending = "pending"
	AccHistoryDeleteRunning = "running"
	AccHistoryDeleteSuccess = "success"
	AccHistoryDeleteFailed  = "failed"
)

// 删除工作器全局只启动一次；容量为 1 的唤醒通道用于合并重复通知。
var (
	accHistoryDeleteWorkerOnce sync.Once
	accHistoryDeleteWake       = make(chan struct{}, 1)
)

// DelAccumulateWithHistoryTasks 在事务中创建历史删除任务，并删除统计器配置、
// 当前计算状态和统计池引用。原始采集数据不在此流程中删除。
func DelAccumulateWithHistoryTasks(list model.IDList) (taskCount int, asyncHistoryDelete bool, err error) {
	ids := normalizeDeleteIDs(list.List)
	if len(ids) == 0 {
		return 0, false, errors.New("empty ids")
	}
	asyncHistoryDelete = config.Conf.Delete.Enabled() && config.Conf.Delete.AsyncDeleteEnabled()

	var accList []model.Accumulate
	if err = common.GetMysql().Model(&model.Accumulate{}).Where("id IN ?", ids).Find(&accList).Error; err != nil {
		return 0, asyncHistoryDelete, err
	}
	accMap := make(map[string]model.Accumulate, len(accList))
	for _, acc := range accList {
		accMap[acc.ID] = acc
	}

	tx := common.GetMysql().Begin()
	if tx.Error != nil {
		return 0, asyncHistoryDelete, tx.Error
	}
	if asyncHistoryDelete {
		taskCount, err = createAccHistoryDeleteTasksTx(tx, ids, accMap)
		if err != nil {
			tx.Rollback()
			return 0, asyncHistoryDelete, err
		}
	}
	if err = tx.Model(&model.Accumulate{}).Where("id IN ?", ids).Delete(&model.Accumulate{}).Error; err != nil {
		tx.Rollback()
		return 0, asyncHistoryDelete, err
	}
	if err = tx.Model(&model.AccStatsValue{}).Where("id IN ?", ids).Delete(&model.AccStatsValue{}).Error; err != nil {
		tx.Rollback()
		return 0, asyncHistoryDelete, err
	}
	if err = tx.Model(&model.StatsDataPoolInfo{}).Where("statsCode IN ?", ids).Delete(&model.StatsDataPoolInfo{}).Error; err != nil {
		tx.Rollback()
		return 0, asyncHistoryDelete, err
	}
	if err = deleteTianmaByIDsTx(tx, ids); err != nil {
		tx.Rollback()
		return 0, asyncHistoryDelete, err
	}
	return taskCount, asyncHistoryDelete, tx.Commit().Error
}

// normalizeDeleteIDs 去除空 ID、首尾空白和重复项。
func normalizeDeleteIDs(ids []string) []string {
	seen := map[string]bool{}
	var data []string
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		data = append(data, id)
	}
	return data
}

// createAccHistoryDeleteTasksTx 在配置删除事务内创建或重置历史删除任务。
// 唯一键冲突时会恢复为 pending，使此前失败或已完成的同 ID 任务可以再次执行。
func createAccHistoryDeleteTasksTx(tx *gorm.DB, ids []string, accMap map[string]model.Accumulate) (int, error) {
	measurement := strings.TrimSpace(config.Conf.Influx.HistoryMeasurement)
	if measurement == "" {
		measurement = "StatsAcc"
	}
	maxRetry := config.Conf.Delete.MaxRetry
	if maxRetry <= 0 {
		maxRetry = 5
	}
	now := time.Now()
	for _, id := range ids {
		acc := accMap[id]
		task := model.AccHistoryDeleteTask{
			StatsID:     id,
			StatsName:   acc.StatsName,
			StatsType:   acc.Type,
			Measurement: measurement,
			Status:      AccHistoryDeletePending,
			RetryCount:  0,
			MaxRetry:    maxRetry,
			LastError:   "",
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "stats_id"}, {Name: "measurement"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"stats_name":  task.StatsName,
				"stats_type":  task.StatsType,
				"status":      AccHistoryDeletePending,
				"retry_count": 0,
				"max_retry":   maxRetry,
				"last_error":  "",
				"finished_at": gorm.Expr("NULL"),
				"updated_at":  now,
			}),
		}).Create(&task).Error
		if err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

// StartAccHistoryDeleteWorker 按配置启动唯一的异步历史删除工作器。
func StartAccHistoryDeleteWorker() {
	if !config.Conf.Delete.Enabled() || !config.Conf.Delete.AsyncDeleteEnabled() {
		return
	}
	accHistoryDeleteWorkerOnce.Do(func() {
		go accHistoryDeleteWorker()
		NotifyAccHistoryDeleteWorker()
	})
}

// NotifyAccHistoryDeleteWorker 非阻塞唤醒删除工作器；连续通知会被合并。
func NotifyAccHistoryDeleteWorker() {
	select {
	case accHistoryDeleteWake <- struct{}{}:
	default:
	}
}

// accHistoryDeleteWorker 在收到通知或达到轮询间隔时处理待执行任务。
func accHistoryDeleteWorker() {
	interval := time.Duration(config.Conf.Delete.WorkerIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	for {
		processAccHistoryDeleteTasks()
		select {
		case <-accHistoryDeleteWake:
		case <-time.After(interval):
		}
	}
}

// processAccHistoryDeleteTasks 取一批可运行任务并按配置并发执行。
// 达到批大小说明可能还有积压，因此立即触发下一轮而不等待定时器。
func processAccHistoryDeleteTasks() {
	tasks := findRunnableAccHistoryDeleteTasks()
	if len(tasks) == 0 {
		return
	}
	runAccHistoryDeleteWithConcurrency(tasks, config.Conf.Delete.MaxConcurrency, handleAccHistoryDeleteTask)
	if len(tasks) >= accHistoryDeleteBatchSize() {
		NotifyAccHistoryDeleteWorker()
	}
}

// findRunnableAccHistoryDeleteTasks 获取待处理、异常中断或已到重试时间的失败任务。
// running 任务会被重新获取，用于服务断电重启后的自动恢复。
func findRunnableAccHistoryDeleteTasks() []model.AccHistoryDeleteTask {
	batchSize := accHistoryDeleteBatchSize()
	interval := time.Duration(config.Conf.Delete.WorkerIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	retryBefore := time.Now().Add(-interval)
	var tasks []model.AccHistoryDeleteTask
	err := common.GetMysql().Model(&model.AccHistoryDeleteTask{}).
		Where("status IN ? OR (status = ? AND retry_count < max_retry AND updated_at <= ?)",
			[]string{AccHistoryDeletePending, AccHistoryDeleteRunning}, AccHistoryDeleteFailed, retryBefore).
		Order("id asc").
		Limit(batchSize).
		Find(&tasks).Error
	if err != nil {
		logger.Error("find acc history delete tasks failed", err)
	}
	return tasks
}

// accHistoryDeleteBatchSize 返回单轮删除任务读取上限。
func accHistoryDeleteBatchSize() int {
	if config.Conf.Delete.BatchSize > 0 {
		return config.Conf.Delete.BatchSize
	}
	return 50
}

// runAccHistoryDeleteWithConcurrency 通过固定数量工作协程限制并发删除压力。
func runAccHistoryDeleteWithConcurrency(tasks []model.AccHistoryDeleteTask, limit int, worker func(model.AccHistoryDeleteTask)) {
	if len(tasks) == 0 {
		return
	}
	if limit <= 0 {
		limit = 2
	}
	size := limit
	if size > len(tasks) {
		size = len(tasks)
	}
	var wg sync.WaitGroup
	indexCh := make(chan model.AccHistoryDeleteTask)
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range indexCh {
				worker(task)
			}
		}()
	}
	for _, task := range tasks {
		indexCh <- task
	}
	close(indexCh)
	wg.Wait()
}

// handleAccHistoryDeleteTask 执行单个任务并持久化 running、failed 或 success 状态。
// 删除失败只增加重试次数，不会阻塞接口或回滚已删除的配置数据。
func handleAccHistoryDeleteTask(task model.AccHistoryDeleteTask) {
	now := time.Now()
	if task.MaxRetry <= 0 {
		task.MaxRetry = config.Conf.Delete.MaxRetry
		if task.MaxRetry <= 0 {
			task.MaxRetry = 5
		}
	}
	if err := common.GetMysql().Model(&model.AccHistoryDeleteTask{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
		"status":     AccHistoryDeleteRunning,
		"updated_at": now,
	}).Error; err != nil {
		logger.Error("mark acc history delete task running failed", err)
		return
	}

	if err := DeleteHistoryByID(task.StatsID, task.Measurement); err != nil {
		retryCount := task.RetryCount + 1
		update := map[string]interface{}{
			"status":      AccHistoryDeleteFailed,
			"retry_count": retryCount,
			"last_error":  err.Error(),
			"updated_at":  time.Now(),
		}
		if updateErr := common.GetMysql().Model(&model.AccHistoryDeleteTask{}).Where("id = ?", task.ID).Updates(update).Error; updateErr != nil {
			logger.Error("mark acc history delete task failed failed", updateErr)
		}
		logger.Error("delete acc history failed", err)
		return
	}

	finishedAt := time.Now()
	if err := common.GetMysql().Model(&model.AccHistoryDeleteTask{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
		"status":      AccHistoryDeleteSuccess,
		"last_error":  "",
		"finished_at": finishedAt,
		"updated_at":  finishedAt,
	}).Error; err != nil {
		logger.Error("mark acc history delete task success failed", err)
	}
}

// DeleteHistoryByID 删除指定统计器在历史 measurement 中的全部统计结果。
// 该操作不会访问或删除网关 measurement 中的原始采集数据。
func DeleteHistoryByID(id, measurement string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("empty stats id")
	}
	measurement = strings.TrimSpace(measurement)
	if measurement == "" {
		measurement = config.Conf.Influx.HistoryMeasurement
	}
	if measurement == "" {
		measurement = "StatsAcc"
	}
	influx, err := newDeleteInfluxClient()
	if err != nil {
		return err
	}
	defer influx.Close()

	cmd := fmt.Sprintf("DELETE FROM \"%s\" WHERE id = '%s'", escapeInfluxIdentifier(measurement), escapeInfluxString(id))
	res, err := influx.Query(client.NewQuery(cmd, config.Conf.Influx.Database, ""))
	if err != nil {
		return err
	}
	if res != nil && res.Error() != nil {
		return res.Error()
	}
	return nil
}

// newDeleteInfluxClient 创建删除任务专用客户端，使长删除超时与实时统计查询隔离。
func newDeleteInfluxClient() (client.Client, error) {
	cfg := config.Conf.Influx
	timeout := time.Duration(config.Conf.Delete.InfluxTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return client.NewHTTPClient(client.HTTPConfig{
		Addr:     fmt.Sprintf("http://%s:%s", cfg.Host, cfg.Port),
		Username: cfg.Username,
		Password: cfg.Password,
		Timeout:  timeout,
	})
}

// escapeInfluxIdentifier 转义双引号包裹的 InfluxQL 标识符。
func escapeInfluxIdentifier(s string) string {
	return strings.ReplaceAll(s, `"`, `\"`)
}
