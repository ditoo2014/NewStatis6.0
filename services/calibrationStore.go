package services

import (
	"NewStatis6.0/model"
	"context"
	"encoding/json"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CalibrationStore 所有事务只写审计专用表，生产配置只读。
type CalibrationStore struct {
	DB      *gorm.DB
	Timeout time.Duration
}

func (s CalibrationStore) Session(ctx context.Context) (*gorm.DB, context.CancelFunc) {
	c, cancel := context.WithTimeout(ctx, s.Timeout)
	return s.DB.WithContext(c), cancel
}

func (s CalibrationStore) Init(ctx context.Context) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	if err := db.AutoMigrate(&model.CalibrationCursor{}, &model.CalibrationTask{}, &model.CalibrationDetail{}); err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&model.CalibrationCursor{ID: 1}).Error
}

// Recover 只能在取得本轮数据库命名锁后调用，恢复断电留下的在途任务。
func (s CalibrationStore) Recover(ctx context.Context) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	return db.Model(&model.CalibrationTask{}).Where("status = ?", "running").Updates(map[string]interface{}{"status": "pending"}).Error
}

func (s CalibrationStore) Cursor(ctx context.Context) (model.CalibrationCursor, error) {
	db, cancel := s.Session(ctx)
	defer cancel()
	var c model.CalibrationCursor
	err := db.First(&c, 1).Error
	return c, err
}

// Scan 原子保存新任务与扫描位置，避免断电造成游标越过未创建的任务。
func (s CalibrationStore) Scan(ctx context.Context, limit int, nextScan int64, build func(model.Accumulate) (*model.CalibrationTask, error)) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	return db.Transaction(func(tx *gorm.DB) error {
		var cursor model.CalibrationCursor
		if err := tx.First(&cursor, 1).Error; err != nil {
			return err
		}
		var accs []model.Accumulate
		if err := tx.Where("id > ?", cursor.LastID).Order("id").Limit(limit).Find(&accs).Error; err != nil {
			return err
		}
		for _, acc := range accs {
			cursor.LastID = acc.ID
			task, err := build(acc)
			if err != nil {
				return err
			}
			if task == nil {
				continue
			}
			var count int64
			if err = tx.Model(&model.CalibrationTask{}).Where("stats_id = ? AND (status IN ? OR (`start` = ? AND `end` = ? AND source_hash = ? AND status IN ?))", acc.ID, []string{"pending", "running"}, task.Start, task.End, task.SourceHash, []string{"done", "failed"}).Count(&count).Error; err != nil {
				return err
			}
			if count == 0 {
				if err = tx.Create(task).Error; err != nil {
					return err
				}
			}
		}
		if len(accs) < limit {
			cursor.LastID = ""
		}
		return tx.Model(&cursor).Updates(map[string]interface{}{"last_id": cursor.LastID, "next_scan": nextScan}).Error
	})
}

func (s CalibrationStore) Pending(ctx context.Context, now int64, limit int) ([]model.CalibrationTask, error) {
	db, cancel := s.Session(ctx)
	defer cancel()
	var tasks []model.CalibrationTask
	err := db.Where("status = ? AND next_attempt <= ?", "pending", now).Order("id").Limit(limit).Find(&tasks).Error
	return tasks, err
}

func (s CalibrationStore) Claim(ctx context.Context, tasks []model.CalibrationTask) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	ids := make([]uint64, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.ID)
	}
	return db.Model(&model.CalibrationTask{}).Where("id IN ? AND status = ?", ids, "pending").Updates(map[string]interface{}{"status": "running", "attempts": gorm.Expr("attempts + 1")}).Error
}

// Finish 在同一个短事务中写明细和完成标记；重试不会产生重复明细。
func (s CalibrationStore) Finish(ctx context.Context, task model.CalibrationTask, details []model.CalibrationDetail, status string) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	return db.Transaction(func(tx *gorm.DB) error {
		if len(details) > 0 {
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&details).Error; err != nil {
				return err
			}
		}
		return tx.Model(&model.CalibrationTask{}).Where("id = ?", task.ID).Updates(map[string]interface{}{"status": status, "finished_at": time.Now(), "last_error": ""}).Error
	})
}

func (s CalibrationStore) Fail(ctx context.Context, tasks []model.CalibrationTask, cause error, retries int, next int64) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	return db.Transaction(func(tx *gorm.DB) error {
		for _, t := range tasks {
			status := "pending"
			if t.Attempts+1 >= retries+1 {
				status = "failed"
			}
			if err := tx.Model(&model.CalibrationTask{}).Where("id = ? AND status = ?", t.ID, "running").Updates(map[string]interface{}{"status": status, "next_attempt": next, "last_error": cause.Error()}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// Yield 轮次预算耗尽属于正常让出，恢复待执行状态，不消耗数据库故障重试次数。
func (s CalibrationStore) Yield(ctx context.Context, tasks []model.CalibrationTask, next int64) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	ids := make([]uint64, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return db.Model(&model.CalibrationTask{}).Where("id IN ? AND status = ?", ids, "running").Updates(map[string]interface{}{
		"status": "pending", "next_attempt": next, "attempts": gorm.Expr("GREATEST(attempts - 1, 0)"),
	}).Error
}

func (s CalibrationStore) Configs(ctx context.Context, tasks []model.CalibrationTask) (map[string]model.Accumulate, error) {
	db, cancel := s.Session(ctx)
	defer cancel()
	ids := make([]string, 0, len(tasks))
	for _, t := range tasks {
		ids = append(ids, t.StatsID)
	}
	var accs []model.Accumulate
	if err := db.Where("id IN ?", ids).Find(&accs).Error; err != nil {
		return nil, err
	}
	result := make(map[string]model.Accumulate)
	for _, a := range accs {
		result[a.ID] = a
	}
	return result, nil
}

func (s CalibrationStore) SaveHealth(ctx context.Context, errors int, pauseUntil int64) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	return db.Model(&model.CalibrationCursor{}).Where("id = 1").Updates(map[string]interface{}{"consecutive_errors": errors, "pause_until": pauseUntil}).Error
}

// Cleanup 每轮有界清理已完成记录；失败和待执行记录保留，明细与任务一起删除。
func (s CalibrationStore) Cleanup(ctx context.Context, before time.Time, limit int) error {
	db, cancel := s.Session(ctx)
	defer cancel()
	return db.Transaction(func(tx *gorm.DB) error {
		var ids []uint64
		if err := tx.Model(&model.CalibrationTask{}).Where("finished_at < ? AND status IN ?", before, []string{"done", "stale"}).Order("id").Limit(limit).Pluck("id", &ids).Error; err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if err := tx.Where("task_id IN ?", ids).Delete(&model.CalibrationDetail{}).Error; err != nil {
			return err
		}
		return tx.Where("id IN ?", ids).Delete(&model.CalibrationTask{}).Error
	})
}

// CalibrationSnapshot 解码任务创建时固定的配置，禁止随当前时间重新选择范围。
func CalibrationSnapshot(task model.CalibrationTask) (model.Accumulate, error) {
	var acc model.Accumulate
	err := json.Unmarshal([]byte(task.Snapshot), &acc)
	return acc, err
}
