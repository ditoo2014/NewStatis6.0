package model

import "time"

// CalibrationCursor 持久化扫描位置和故障暂停时间，不占用生产统计游标。
type CalibrationCursor struct {
	ID                uint   `gorm:"primaryKey"`
	LastID            string `gorm:"type:varchar(100)"`
	NextScan          int64
	ConsecutiveErrors int
	PauseUntil        int64
}

func (CalibrationCursor) TableName() string { return "acc_calibration_cursor" }

// CalibrationTask 固定每次审计的配置快照和周期，断电后重试同一范围。
type CalibrationTask struct {
	ID          uint64 `gorm:"primaryKey;autoIncrement"`
	StatsID     string `gorm:"type:varchar(100);index"`
	Snapshot    string `gorm:"type:text"`
	SourceHash  string `gorm:"type:varchar(64)"`
	Start       int64
	End         int64
	Status      string `gorm:"type:varchar(24);index:idx_calibration_due,priority:1"`
	Attempts    int
	NextAttempt int64  `gorm:"index:idx_calibration_due,priority:2"`
	LastError   string `gorm:"type:text"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	FinishedAt  *time.Time `gorm:"index"`
}

func (CalibrationTask) TableName() string { return "acc_calibration_task" }

// CalibrationDetail 保留原始边界、候选值及既有历史值；不写入报表历史表。
type CalibrationDetail struct {
	TaskID      uint64 `gorm:"primaryKey;autoIncrement:false"`
	PeriodStart int64  `gorm:"primaryKey;autoIncrement:false"`
	PeriodEnd   int64
	StartTime   int64
	StartValue  float64 `gorm:"type:double"`
	EndTime     int64
	EndValue    float64 `gorm:"type:double"`
	HasData     bool
	Expected    float64  `gorm:"type:double"`
	Actual      *float64 `gorm:"type:double"`
	Status      string   `gorm:"type:varchar(32)"`
}

func (CalibrationDetail) TableName() string { return "acc_calibration_detail" }
