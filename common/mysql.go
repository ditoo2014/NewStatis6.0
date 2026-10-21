// Package common 管理 MySQL 与 InfluxDB 等进程级共享基础设施。
package common

import (
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"fmt"
	"net/url"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// DB 是应用共享的 GORM 数据库连接。
var DB *gorm.DB

// InitMysql 按配置连接 MySQL、设置连接池，并确保所需表结构存在。
// 连接失败时会尝试创建数据库并按配置间隔重试。
func InitMysql() error {
	cfg := config.Conf.MySQL
	loc := url.QueryEscape(config.Conf.Server.Timezone)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database, loc)

	var err error
	for i := 0; i < cfg.RetryCount; i++ {
		DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err == nil {
			sqlDB, _ := DB.DB()
			sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
			sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
			sqlDB.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeMinutes) * time.Minute)
			err = DB.AutoMigrate(&model.Accumulate{}, &model.AccStatsValue{}, &model.AccHistoryDeleteTask{}, &model.StatsDataPool{}, &model.StatsDataPoolInfo{})
			if err != nil {
				return err
			}
			if err = initStatsCodeSequence(DB); err != nil {
				return fmt.Errorf("初始化统计编号表失败: %w", err)
			}
			initStatsPoolIndexes(DB)
			logger.Info("MySQL connected", nil)
			return nil
		}
		logger.Error("MySQL connect failed", err)
		createDatabase(cfg)
		time.Sleep(time.Duration(cfg.RetryIntervalSeconds) * time.Second)
	}
	return err
}

// createDatabase 在目标数据库不存在时通过无库名连接尝试创建它。
// 创建失败由后续连接重试统一处理，不在此处中断启动流程。
func createDatabase(cfg config.MySQLConfig) {
	loc := url.QueryEscape(config.Conf.Server.Timezone)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=utf8mb4&parseTime=True&loc=%s", cfg.Username, cfg.Password, cfg.Host, cfg.Port, loc)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return
	}
	db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", cfg.Database))
}

// GetMysql 返回应用共享的 GORM 连接。
func GetMysql() *gorm.DB {
	return DB
}
