// Package config 负责读取应用配置、补全默认值并初始化统一时区。
package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// AppConfig 聚合服务运行所需的全部配置分组。
type AppConfig struct {
	Server      ServerConfig      `yaml:"server"`
	Log         LogConfig         `yaml:"log"`
	MySQL       MySQLConfig       `yaml:"mysql"`
	Influx      InfluxConfig      `yaml:"influx"`
	CJY         CJYConfig         `yaml:"cjy"`
	Stats       StatsConfig       `yaml:"stats"`
	Delete      DeleteConfig      `yaml:"delete_task"`
	Calibration CalibrationConfig `yaml:"calibration"`
}

// ServerConfig 定义 HTTP 服务端口和业务时间所使用的时区。
type ServerConfig struct {
	Port     int    `yaml:"port"`
	Timezone string `yaml:"timezone"`
}

// LogConfig 定义日志级别、文件路径及滚动清理策略。
type LogConfig struct {
	Path       string `yaml:"path"`
	Level      string `yaml:"level"`
	MaxSizeMB  int    `yaml:"max_size_mb"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAgeDays int    `yaml:"max_age_days"`
	Compress   bool   `yaml:"compress"`
}

// MySQLConfig 定义 MySQL 连接、连接池和启动重试参数。
type MySQLConfig struct {
	Host                   string `yaml:"host"`
	Port                   string `yaml:"port"`
	Username               string `yaml:"username"`
	Password               string `yaml:"password"`
	Database               string `yaml:"database"`
	MaxOpenConns           int    `yaml:"max_open_conns"`
	MaxIdleConns           int    `yaml:"max_idle_conns"`
	ConnMaxLifetimeMinutes int    `yaml:"conn_max_lifetime_minutes"`
	RetryCount             int    `yaml:"retry_count"`
	RetryIntervalSeconds   int    `yaml:"retry_interval_seconds"`
}

// InfluxConfig 定义 InfluxDB 连接以及原始数据、统计历史数据的 measurement 规则。
type InfluxConfig struct {
	Host                      string `yaml:"host"`
	Port                      string `yaml:"port"`
	Username                  string `yaml:"username"`
	Password                  string `yaml:"password"`
	Database                  string `yaml:"database"`
	HistoryMeasurement        string `yaml:"history_measurement"`
	RawMeasurementFromGateway bool   `yaml:"raw_measurement_from_gateway"`
	Precision                 string `yaml:"precision"`
	RetryCount                int    `yaml:"retry_count"`
	RetryIntervalSeconds      int    `yaml:"retry_interval_seconds"`
}

// CJYConfig 定义采集云接口的地址和登录凭据。
type CJYConfig struct {
	BaseURL  string `yaml:"base_url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// StatsConfig 定义统计调度、历史补算、批处理及异常数据处理策略。
type StatsConfig struct {
	Enable                    bool   `yaml:"enable"`
	IntervalSeconds           int    `yaml:"interval_seconds"`
	MaxConcurrency            int    `yaml:"max_concurrency"`
	BackfillConcurrency       int    `yaml:"backfill_concurrency"`
	CatchUpOnStart            bool   `yaml:"catch_up_on_start"`
	CatchUpBatchSize          int    `yaml:"catch_up_batch_size"`
	SettleDelaySeconds        int    `yaml:"settle_delay_seconds"`
	QueryTimeoutSeconds       int    `yaml:"query_timeout_seconds"`
	WriteTimeoutSeconds       int    `yaml:"write_timeout_seconds"`
	SourceIDBatchSize         int    `yaml:"source_id_batch_size"`
	StateUpsertBatchSize      int    `yaml:"state_upsert_batch_size"`
	HistoryWriteBatchSize     int    `yaml:"history_write_batch_size"`
	WriteZeroWhenPeriodNoData bool   `yaml:"write_zero_when_period_no_data"`
	RealtimeZeroWhenNoData    bool   `yaml:"realtime_zero_when_no_current_data"`
	NegativeDeltaPolicy       string `yaml:"negative_delta_policy"`
}

// DeleteConfig 定义统计历史异步删除任务的执行策略。
// 布尔项使用指针，以区分“未配置”和显式配置为 false。
type DeleteConfig struct {
	Enable                *bool `yaml:"enable"`
	AsyncHistoryDelete    *bool `yaml:"async_history_delete"`
	WorkerIntervalSeconds int   `yaml:"worker_interval_seconds"`
	MaxConcurrency        int   `yaml:"max_concurrency"`
	MaxRetry              int   `yaml:"max_retry"`
	InfluxTimeoutSeconds  int   `yaml:"influx_timeout_seconds"`
	BatchSize             int   `yaml:"batch_size"`
}

// Enabled 返回删除任务是否启用；配置缺失时默认启用。
func (c DeleteConfig) Enabled() bool {
	return boolDefault(c.Enable, true)
}

// AsyncDeleteEnabled 返回删除统计器时是否采用异步历史清理。
func (c DeleteConfig) AsyncDeleteEnabled() bool {
	return boolDefault(c.AsyncHistoryDelete, true)
}

// boolDefault 在布尔配置未填写时返回指定的默认值。
func boolDefault(v *bool, defaultValue bool) bool {
	if v == nil {
		return defaultValue
	}
	return *v
}

// Conf 保存应用启动后生效的全局配置。
var Conf AppConfig

// Location 保存业务统一使用的时区，默认由配置设为北京时间。
var Location *time.Location

// LoadConfig 从 YAML 文件加载配置、应用默认值并解析业务时区。
func LoadConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(data, &Conf); err != nil {
		return err
	}
	applyDefaults()
	Location, err = time.LoadLocation(Conf.Server.Timezone)
	if err != nil {
		return err
	}
	return nil
}

// applyDefaults 为未显式填写的配置项补充保守的运行默认值。
func applyDefaults() {
	Conf.Calibration.Defaults()
	if Conf.Server.Port == 0 {
		Conf.Server.Port = 8014
	}
	if Conf.Server.Timezone == "" {
		Conf.Server.Timezone = "Asia/Shanghai"
	}
	if Conf.Log.Path == "" {
		Conf.Log.Path = "./logs/newstats.log"
	}
	if Conf.Log.MaxSizeMB == 0 {
		Conf.Log.MaxSizeMB = 100
	}
	if Conf.Log.MaxBackups == 0 {
		Conf.Log.MaxBackups = 30
	}
	if Conf.Log.MaxAgeDays == 0 {
		Conf.Log.MaxAgeDays = 30
	}
	if Conf.MySQL.Host == "" {
		Conf.MySQL.Host = "localhost"
	}
	if Conf.MySQL.Port == "" {
		Conf.MySQL.Port = "3306"
	}
	if Conf.MySQL.RetryCount == 0 {
		Conf.MySQL.RetryCount = 10
	}
	if Conf.MySQL.RetryIntervalSeconds == 0 {
		Conf.MySQL.RetryIntervalSeconds = 10
	}
	if Conf.Influx.Host == "" {
		Conf.Influx.Host = "localhost"
	}
	if Conf.Influx.Port == "" {
		Conf.Influx.Port = "8086"
	}
	if Conf.Influx.HistoryMeasurement == "" {
		Conf.Influx.HistoryMeasurement = "StatsAcc"
	}
	if Conf.Influx.Database == "" {
		Conf.Influx.Database = "tolink"
	}
	if Conf.Influx.Precision == "" {
		Conf.Influx.Precision = "ns"
	}
	if Conf.CJY.BaseURL == "" {
		Conf.CJY.BaseURL = "localhost:83"
	}
	if Conf.CJY.Username == "" {
		Conf.CJY.Username = "ditoo"
	}
	if Conf.CJY.Password == "" {
		Conf.CJY.Password = "ditoo_password"
	}
	if Conf.Stats.IntervalSeconds == 0 {
		Conf.Stats.IntervalSeconds = 60
	}
	if Conf.Stats.MaxConcurrency == 0 {
		Conf.Stats.MaxConcurrency = 20
	}
	if Conf.Stats.BackfillConcurrency == 0 {
		Conf.Stats.BackfillConcurrency = 3
	}
	if Conf.Stats.CatchUpBatchSize == 0 {
		Conf.Stats.CatchUpBatchSize = 200
	}
	if Conf.Stats.QueryTimeoutSeconds == 0 {
		Conf.Stats.QueryTimeoutSeconds = 15
	}
	if Conf.Stats.WriteTimeoutSeconds == 0 {
		Conf.Stats.WriteTimeoutSeconds = 15
	}
	if Conf.Stats.SourceIDBatchSize == 0 {
		Conf.Stats.SourceIDBatchSize = 300
	}
	if Conf.Stats.StateUpsertBatchSize == 0 {
		Conf.Stats.StateUpsertBatchSize = 500
	}
	if Conf.Stats.HistoryWriteBatchSize == 0 {
		Conf.Stats.HistoryWriteBatchSize = 2000
	}
	if Conf.Stats.NegativeDeltaPolicy == "" {
		Conf.Stats.NegativeDeltaPolicy = "zero"
	}
	if Conf.Delete.WorkerIntervalSeconds == 0 {
		Conf.Delete.WorkerIntervalSeconds = 30
	}
	if Conf.Delete.MaxConcurrency == 0 {
		Conf.Delete.MaxConcurrency = 2
	}
	if Conf.Delete.MaxRetry == 0 {
		Conf.Delete.MaxRetry = 5
	}
	if Conf.Delete.InfluxTimeoutSeconds == 0 {
		Conf.Delete.InfluxTimeoutSeconds = 60
	}
	if Conf.Delete.BatchSize == 0 {
		Conf.Delete.BatchSize = 50
	}
}
