package common

import (
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"fmt"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

// Influx 是应用共享的 InfluxDB HTTP 客户端。
var Influx client.Client

// InitInfluxDB 使用统计读写超时中的较大值创建 InfluxDB 客户端，并按配置重试。
func InitInfluxDB() error {
	cfg := config.Conf.Influx
	timeoutSeconds := config.Conf.Stats.QueryTimeoutSeconds
	if config.Conf.Stats.WriteTimeoutSeconds > timeoutSeconds {
		timeoutSeconds = config.Conf.Stats.WriteTimeoutSeconds
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}
	var err error
	for i := 0; i < cfg.RetryCount; i++ {
		Influx, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     fmt.Sprintf("http://%s:%s", cfg.Host, cfg.Port),
			Username: cfg.Username,
			Password: cfg.Password,
			Timeout:  time.Duration(timeoutSeconds) * time.Second,
		})
		if err == nil {
			logger.Info("InfluxDB connected", nil)
			return nil
		}
		logger.Error("InfluxDB connect failed", err)
		time.Sleep(time.Duration(cfg.RetryIntervalSeconds) * time.Second)
	}
	return err
}

// GetInfluxDB 返回应用共享的 InfluxDB 客户端。
func GetInfluxDB() client.Client {
	return Influx
}

// CloseInflux 释放 InfluxDB 客户端持有的网络资源。
func CloseInflux() {
	if Influx != nil {
		_ = Influx.Close()
	}
}
