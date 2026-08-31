// Package logger 提供同时输出到控制台和滚动文件的统一日志入口。
package logger

import (
	"NewStatis6.0/config"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mattn/go-colorable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger 是完成初始化后的全局 Zap 日志器。
var Logger *zap.Logger

// InitLog 创建日志目录，并按配置初始化控制台输出和自动滚动文件输出。
func InitLog() error {
	path := config.Conf.Log.Path
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:       "time",
		LevelKey:      "level",
		CallerKey:     "caller",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.In(config.Location).Format("2006/01/02 15:04:05"))
		},
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	level := logLevel(config.Conf.Log.Level)
	fileWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path,
		MaxSize:    config.Conf.Log.MaxSizeMB,
		MaxBackups: config.Conf.Log.MaxBackups,
		MaxAge:     config.Conf.Log.MaxAgeDays,
		Compress:   config.Conf.Log.Compress,
	})
	core := zapcore.NewTee(
		zapcore.NewCore(encoder, zapcore.AddSync(colorable.NewColorableStdout()), level),
		zapcore.NewCore(encoder, fileWriter, level),
	)
	Logger = zap.New(core, zap.AddCaller())
	return nil
}

// logLevel 将配置中的文本级别转换为 Zap 日志级别。
func logLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zap.DebugLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}

// Info 记录带附加数据的普通运行日志。
func Info(msg string, data interface{}) {
	if Logger != nil {
		Logger.Info(fmt.Sprintf("%s data: %v", msg, data))
	}
}

// Error 记录错误及其调用位置。
func Error(msg string, err error) {
	if Logger != nil {
		Logger.Error(msg, zap.Error(err))
	}
}

// Warn 记录需要关注但不阻断当前流程的情况。
func Warn(msg string, data interface{}) {
	if Logger != nil {
		Logger.Warn(fmt.Sprintf("%s data: %v", msg, data))
	}
}
