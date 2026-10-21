package common

import (
	"NewStatis6.0/logger"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// Separate index declarations keep failures out of the startup AutoMigrate:
// duplicate production records must not prevent other service functions starting.
type statsPoolIndexSchema struct {
	PoolName string `gorm:"column:poolName;uniqueIndex:uq_pool_name"`
	ApiKey   string `gorm:"column:apiKey;uniqueIndex:uq_pool_api_key"`
}

func (statsPoolIndexSchema) TableName() string { return "stats_data_pool" }

type statsPoolLabelIndexSchema struct {
	LabelID   string `gorm:"column:labelID;uniqueIndex:uq_pool_label_id"`
	PoolID    uint   `gorm:"column:poolID;uniqueIndex:uq_pool_xh,priority:1;uniqueIndex:uq_pool_stats_code,priority:1"`
	XH        uint   `gorm:"column:xh;uniqueIndex:uq_pool_xh,priority:2"`
	StatsCode string `gorm:"column:statsCode;uniqueIndex:uq_pool_stats_code,priority:2"`
}

func (statsPoolLabelIndexSchema) TableName() string { return "stats_data_pool_info" }

// Retry absent indexes on every restart without deleting or renumbering data.
func initStatsPoolIndexes(db *gorm.DB) {
	indexes := []struct {
		model   interface{}
		table   string
		name    string
		columns []string
	}{
		{&statsPoolIndexSchema{}, "stats_data_pool", "uq_pool_name", []string{"poolName"}},
		{&statsPoolIndexSchema{}, "stats_data_pool", "uq_pool_api_key", []string{"apiKey"}},
		{&statsPoolLabelIndexSchema{}, "stats_data_pool_info", "uq_pool_label_id", []string{"labelID"}},
		{&statsPoolLabelIndexSchema{}, "stats_data_pool_info", "uq_pool_xh", []string{"poolID", "xh"}},
		{&statsPoolLabelIndexSchema{}, "stats_data_pool_info", "uq_pool_stats_code", []string{"poolID", "statsCode"}},
	}
	for _, index := range indexes {
		if db.Migrator().HasIndex(index.model, index.name) {
			continue
		}
		// MySQL UNIQUE permits multiple NULLs, so precheck only non-NULL keys.
		query := db.Table(index.table).Select(index.columns)
		conditions := make([]string, 0, len(index.columns))
		for _, column := range index.columns {
			conditions = append(conditions, fmt.Sprintf("`%s` IS NOT NULL", column))
		}
		query = query.Where(strings.Join(conditions, " AND ")).Group("`" + strings.Join(index.columns, "`, `") + "`")
		var duplicate []map[string]interface{}
		if err := query.Having("COUNT(*) > ?", 1).Limit(1).Find(&duplicate).Error; err != nil {
			logger.Error("统计池索引预检失败，跳过 "+index.table+"."+index.name, err)
			continue
		}
		if len(duplicate) > 0 {
			logger.Warn("统计池唯一索引未创建：存在重复数据，请处理后重启；"+index.table+"."+index.name, nil)
			continue
		}
		if err := db.Migrator().CreateIndex(index.model, index.name); err != nil {
			// Another instance may have finished creating the same index.
			if !db.Migrator().HasIndex(index.model, index.name) {
				logger.Error("统计池唯一索引创建失败，服务继续启动；"+index.table+"."+index.name, err)
			}
			continue
		}
		logger.Info("统计池唯一索引已创建："+index.table+"."+index.name, nil)
	}
}
