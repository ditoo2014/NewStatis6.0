// Package model 定义数据库实体以及接口请求、响应结构。
package model

import "time"

// Accumulate 是统计器基础配置，对应生产库中的 accumulate 表。
// Type 使用 0、1、2、3 分别表示半小时、小时、日、月统计周期。
type Accumulate struct {
	ID         string `gorm:"type:varchar(100);column:id;primaryKey" json:"id"`
	GatewayID  string `gorm:"type:varchar(100);column:gatewayID" json:"gatewayID"`
	Point      string `gorm:"type:varchar(100);column:point" json:"point"`
	StatsName  string `gorm:"type:varchar(100);column:statsName" json:"statsName"`
	Type       int    `gorm:"type:integer;column:type" json:"type"`
	StartTime  string `gorm:"type:varchar(100);column:startTime" json:"startTime"`
	DeviceType string `gorm:"type:varchar(50);column:deviceType" json:"deviceType"`
	Count      string `gorm:"type:varchar(50);column:count" json:"count"`
	Notes      string `gorm:"type:varchar(200);column:notes" json:"notes"`
	LargeName  string `gorm:"type:varchar(50);column:largeName" json:"largeName"`
}

// AccStatsValue 保存统计器当前周期的游标和已采集边界值。
// PeriodStartTime、PeriodEndTime 确定待处理周期；Gap 表示前序周期存在数据断档。
type AccStatsValue struct {
	ID              string    `gorm:"type:varchar(50);column:id;primaryKey;uniqueIndex" json:"id"`
	Type            int       `gorm:"type:integer;column:type" json:"type"`
	PeriodStartTime int64     `gorm:"type:bigint;column:period_start_time" json:"period_start_time"`
	PeriodEndTime   int64     `gorm:"type:bigint;column:period_end_time" json:"period_end_time"`
	StartTime       int64     `gorm:"type:bigint;column:start_time" json:"start_time"`
	StartValue      float64   `gorm:"type:decimal(20,4);column:start_value" json:"start_value"`
	EndTime         int64     `gorm:"type:bigint;column:end_time" json:"end_time"`
	EndValue        float64   `gorm:"type:decimal(20,4);column:end_value" json:"end_value"`
	HasData         bool      `gorm:"type:boolean;column:has_data" json:"has_data"`
	Gap             bool      `gorm:"type:boolean;column:gap" json:"gap"`
	CreatedAt       time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt       time.Time `gorm:"column:updated_at" json:"updated_at"`
}

// AccHistoryDeleteTask 持久化删除统计器后异步清理历史统计数据的任务状态。
type AccHistoryDeleteTask struct {
	ID          uint       `gorm:"type:integer;column:id;primaryKey;autoIncrement" json:"id"`
	StatsID     string     `gorm:"type:varchar(100);column:stats_id;uniqueIndex:idx_acc_history_delete_task_stats_measurement" json:"stats_id"`
	StatsName   string     `gorm:"type:varchar(100);column:stats_name" json:"stats_name"`
	StatsType   int        `gorm:"type:integer;column:stats_type" json:"stats_type"`
	Measurement string     `gorm:"type:varchar(100);column:measurement;uniqueIndex:idx_acc_history_delete_task_stats_measurement" json:"measurement"`
	Status      string     `gorm:"type:varchar(20);column:status;index" json:"status"`
	RetryCount  int        `gorm:"type:integer;column:retry_count" json:"retry_count"`
	MaxRetry    int        `gorm:"type:integer;column:max_retry" json:"max_retry"`
	LastError   string     `gorm:"type:text;column:last_error" json:"last_error"`
	CreatedAt   time.Time  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at" json:"updated_at"`
	FinishedAt  *time.Time `gorm:"column:finished_at" json:"finished_at"`
}

// AccPageInfo 是统计器配置列表的分页查询条件。
type AccPageInfo struct {
	Page   int    `json:"page"`
	Size   int    `json:"size"`
	Search string `json:"search"`
}

// AccForm 是统计报表中的一条历史或实时统计结果。
type AccForm struct {
	ID         string  `json:"id"`
	StatsName  string  `json:"statsName"`
	Type       int     `json:"type"`
	Time       string  `json:"time"`
	DeviceType string  `json:"deviceType"`
	Value      float64 `json:"value"`
	Notes      string  `json:"notes"`
}

// AccFormPage 是单组统计报表的筛选、时间范围和分页条件。
type AccFormPage struct {
	IDAndName  string `json:"idAndName"`
	Type       int    `json:"type"`
	Page       int    `json:"page"`
	Size       int    `json:"size"`
	Search     string `json:"search"`
	StartTime  int64  `json:"startTime"`
	EndTime    int64  `json:"endTime"`
	DeviceType string `json:"deviceType"`
	RealTime   bool   `json:"realTime"`
}

// AccFormPageByIDAndName 是按多个“统计器ID,名称”组合查询历史的请求结构。
type AccFormPageByIDAndName struct {
	IDAndNameList []string `json:"idAndNameList"`
	Type          int      `json:"type"`
	Page          int      `json:"page"`
	Size          int      `json:"size"`
	StartTime     int64    `json:"startTime"`
	EndTime       int64    `json:"endTime"`
	RealTime      bool     `json:"realTime"`
}

// AccTimeRange 描述报表汇总使用的时间范围以及是否包含实时值。
type AccTimeRange struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`
	RealTime  bool  `json:"realTime"`
}

// EnergyBoardSummaryReq 定义能源看板日、周、月、年汇总的统计器和时间范围。
type EnergyBoardSummaryReq struct {
	DailyIDAndNameList   []string     `json:"dailyIdAndNameList"`
	MonthlyIDAndNameList []string     `json:"monthlyIdAndNameList"`
	DayRange             AccTimeRange `json:"dayRange"`
	WeekRange            AccTimeRange `json:"weekRange"`
	MonthRange           AccTimeRange `json:"monthRange"`
	YearRange            AccTimeRange `json:"yearRange"`
}

// EnergyBoardSummaryRes 返回能源看板汇总值和本月每日明细。
type EnergyBoardSummaryRes struct {
	Today          float64   `json:"today"`
	Week           float64   `json:"week"`
	Month          float64   `json:"month"`
	Year           float64   `json:"year"`
	MonthDailyList []AccForm `json:"monthDailyList"`
}

// AccOutput 是导出统计器配置时的筛选条件。
type AccOutput struct {
	IDList []string `json:"IDList"`
	Type   int      `json:"type"`
}

// StatsAccOutput 是统计器配置导出文件中的行结构。
type StatsAccOutput struct {
	ID         string `json:"id"`
	StatsName  string `json:"statsName"`
	GatewayID  string `json:"gatewayID"`
	Point      string `json:"point"`
	Type       string `json:"type"`
	StartTime  string `json:"startTime"`
	DeviceType string `json:"deviceType"`
	Count      string `json:"count"`
	Notes      string `json:"notes"`
}

// IDList 是批量删除等接口共用的统计器 ID 列表。
type IDList struct {
	List []string `json:"list"`
}

// TableName 固定统计器配置的生产表名。
func (Accumulate) TableName() string {
	return "accumulate"
}

// TableName 固定统计器当前计算状态表名。
func (AccStatsValue) TableName() string {
	return "acc_stats_value"
}

// TableName 固定统计历史异步删除任务表名。
func (AccHistoryDeleteTask) TableName() string {
	return "acc_history_delete_task"
}
