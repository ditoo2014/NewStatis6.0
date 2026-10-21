package config

import "fmt"

// CalibrationConfig 只控制独立历史审计，不改变实时计算参数。
type CalibrationConfig struct {
	Enable                      bool  `yaml:"enable"`
	AuditOnly                   *bool `yaml:"audit_only"`
	IntervalSeconds             int   `yaml:"interval_seconds"`
	Concurrency                 int   `yaml:"concurrency"`
	StatsBatchSize              int   `yaml:"stats_batch_size"`
	PointBatchSize              int   `yaml:"point_batch_size"`
	RecentPeriods               int   `yaml:"recent_periods"`
	SettleDelaySeconds          int   `yaml:"settle_delay_seconds"`
	MaxTaskSeconds              int   `yaml:"max_task_seconds"`
	QueryTimeoutSeconds         int   `yaml:"query_timeout_seconds"`
	WriteTimeoutSeconds         int   `yaml:"write_timeout_seconds"`
	RetryCount                  *int  `yaml:"retry_count"`
	RetryIntervalSeconds        int   `yaml:"retry_interval_seconds"`
	PauseAfterConsecutiveErrors int   `yaml:"pause_after_consecutive_errors"`
	ErrorPauseSeconds           int   `yaml:"error_pause_seconds"`
	AuditRetentionDays          int   `yaml:"audit_retention_days"`
}

// Defaults 兼容旧 YAML；零值数值项采用默认值，重试次数允许显式设置为 0。
func (c *CalibrationConfig) Defaults() {
	for _, item := range []struct {
		p     *int
		value int
	}{
		{&c.IntervalSeconds, 600}, {&c.Concurrency, 1}, {&c.StatsBatchSize, 100},
		{&c.PointBatchSize, 20}, {&c.RecentPeriods, 1}, {&c.SettleDelaySeconds, 600},
		{&c.MaxTaskSeconds, 120}, {&c.QueryTimeoutSeconds, 15}, {&c.WriteTimeoutSeconds, 15},
		{&c.RetryIntervalSeconds, 60}, {&c.PauseAfterConsecutiveErrors, 3},
		{&c.ErrorPauseSeconds, 600}, {&c.AuditRetentionDays, 90},
	} {
		if *item.p == 0 {
			*item.p = item.value
		}
	}
	if c.RetryCount == nil {
		n := 1
		c.RetryCount = &n
	}
}

func (c CalibrationConfig) IsAuditOnly() bool { return boolDefault(c.AuditOnly, true) }

// Validate 拒绝危险配置，仅停用审计模块，不阻断原有服务。
func (c CalibrationConfig) Validate() error {
	if !c.IsAuditOnly() {
		return fmt.Errorf("calibration: automatic repair is not supported; audit_only must be true")
	}
	for _, n := range []int{c.IntervalSeconds, c.Concurrency, c.StatsBatchSize, c.PointBatchSize, c.RecentPeriods, c.SettleDelaySeconds, c.MaxTaskSeconds, c.QueryTimeoutSeconds, c.WriteTimeoutSeconds, c.RetryIntervalSeconds, c.PauseAfterConsecutiveErrors, c.ErrorPauseSeconds, c.AuditRetentionDays} {
		if n <= 0 {
			return fmt.Errorf("calibration: numeric settings must be positive")
		}
	}
	if c.RetryCount == nil || *c.RetryCount < 0 {
		return fmt.Errorf("calibration: retry_count must be >= 0")
	}
	if c.Concurrency > 16 || c.StatsBatchSize > 1000 || c.PointBatchSize > 100 || c.RecentPeriods > 48 {
		return fmt.Errorf("calibration: limits exceeded (concurrency<=16, stats_batch_size<=1000, point_batch_size<=100, recent_periods<=48)")
	}
	return nil
}
