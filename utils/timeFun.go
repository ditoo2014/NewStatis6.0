// Package utils 提供统计周期、公式计算、标识生成和采集云接口等通用能力。
package utils

import (
	"NewStatis6.0/config"
	"time"
)

// Period 描述一个左闭右开的统计周期 [Start, End)。
// DisplayTime 用于报表展示，NextStart 与 End 保持一致以便推进游标。
type Period struct {
	Type        int
	Start       int64
	End         int64
	DisplayTime string
	NextStart   int64
}

// CurrentTime 返回配置时区下的当前 Unix 秒时间戳。
func CurrentTime() int64 {
	return time.Now().In(config.Location).Unix()
}

// Timestamp 按业务时区解析标准日期时间字符串，格式无效时返回 0。
func Timestamp(timeStr string) int64 {
	layout := "2006-01-02 15:04:05"
	parsedTime, err := time.ParseInLocation(layout, timeStr, config.Location)
	if err != nil {
		return 0
	}
	return parsedTime.Unix()
}

// BuildPeriod 将任意时间归入半小时、小时、日或月周期。
// 月和日边界使用日历运算，能够正确处理不等长月份和时区边界。
func BuildPeriod(typ int, timestamp int64) Period {
	t := time.Unix(timestamp, 0).In(config.Location)
	var start, end time.Time
	switch typ {
	case 0:
		minute := (t.Minute() / 30) * 30
		start = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, config.Location)
		end = start.Add(30 * time.Minute)
	case 1:
		start = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, config.Location)
		end = start.Add(time.Hour)
	case 2:
		start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, config.Location)
		end = start.AddDate(0, 0, 1)
	case 3:
		start = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, config.Location)
		end = start.AddDate(0, 1, 0)
	default:
		start = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, config.Location)
		end = start.Add(time.Hour)
	}
	return Period{Type: typ, Start: start.Unix(), End: end.Unix(), DisplayTime: FormatPeriodTime(typ, start.Unix()), NextStart: end.Unix()}
}

// NextPeriod 根据当前周期结束时间构造紧邻的下一个周期。
func NextPeriod(period Period) Period {
	return BuildPeriod(period.Type, period.End)
}

// FormatPeriodTime 按统计类型生成与既有接口兼容的周期显示时间。
func FormatPeriodTime(typ int, timestamp int64) string {
	t := time.Unix(timestamp, 0).In(config.Location)
	switch typ {
	case 0:
		return t.Format("2006-01-02 15:04")
	case 1:
		return t.Format("2006-01-02 15")
	case 2:
		return t.Format("2006-01-02")
	case 3:
		return t.Format("2006-01")
	default:
		return t.Format("2006-01-02 15:04")
	}
}

// SanMonthTime 返回业务时区中当前时刻向前一个月的 Unix 秒时间戳。
func SanMonthTime() int64 {
	return time.Now().In(config.Location).AddDate(0, -1, 0).Unix()
}

// 以下四个快捷方法分别返回时间所在半小时、小时、自然日和自然月周期的起点。
func CurrentHalfHourStart(timestamp int64) int64 { return BuildPeriod(0, timestamp).Start }
func HourTimestamp(timestamp int64) int64        { return BuildPeriod(1, timestamp).Start }
func DayTimestamp(timestamp int64) int64         { return BuildPeriod(2, timestamp).Start }
func MonthTimestamp(timestamp int64) int64       { return BuildPeriod(3, timestamp).Start }

// NextHalfHourTimestamp 返回时间所在半小时周期的终点。
func NextHalfHourTimestamp(timestamp int64) int64 {
	return BuildPeriod(0, timestamp).End
}

// 以下三个快捷方法分别返回时间所在小时、自然日和自然月周期的终点。
func HourNextTimestamp(timestamp int64) int64  { return BuildPeriod(1, timestamp).End }
func DayNextTimestamp(timestamp int64) int64   { return BuildPeriod(2, timestamp).End }
func MonthNextTimestamp(timestamp int64) int64 { return BuildPeriod(3, timestamp).End }

// MonthNext2Timestamp 返回当前自然月之后第二个月的起点，即下一个月周期的终点。
func MonthNext2Timestamp(timestamp int64) int64 {
	return BuildPeriod(3, BuildPeriod(3, timestamp).End).End
}
