package services

import (
	"NewStatis6.0/config"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	client "github.com/influxdata/influxdb1-client/v2"
)

// CalibrationHistory 保留类型，避免把其他统计类型的历史误判为相同结果。
type CalibrationHistory struct {
	Value float64
	Type  int
}

// ReadCalibrationHistory 使用独立连接读取有界范围；不存在与解析失败严格区分。
func ReadCalibrationHistory(conn client.Client, ids []string, start, end int64) (map[string]map[int64]CalibrationHistory, error) {
	result := make(map[string]map[int64]CalibrationHistory)
	if len(ids) == 0 {
		return result, nil
	}
	cmd := fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE (%s) and time >= %d000000000 and time < %d000000000 GROUP BY id", escapeInfluxIdentifier(config.Conf.Influx.HistoryMeasurement), buildInfluxIDFilter(ids), start, end)
	res, err := conn.Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("nil calibration history response")
	}
	if err = res.Error(); err != nil {
		return nil, err
	}
	if len(res.Results) != 1 {
		return nil, fmt.Errorf("unexpected calibration history result count")
	}
	for _, series := range res.Results[0].Series {
		id := series.Tags["id"]
		if id == "" {
			return nil, fmt.Errorf("history series has no id")
		}
		columns := map[string]int{}
		for i, name := range series.Columns {
			columns[name] = i
		}
		for _, row := range series.Values {
			get := func(name string) (interface{}, error) {
				i, ok := columns[name]
				if !ok || i >= len(row) {
					return nil, fmt.Errorf("missing history column %s", name)
				}
				return row[i], nil
			}
			tr, e := get("time")
			if e != nil {
				return nil, e
			}
			ts, ok := parseInfluxTimestamp([]interface{}{tr})
			if !ok || ts < start || ts >= end {
				return nil, fmt.Errorf("invalid history timestamp")
			}
			vr, e := get("data")
			if e != nil {
				return nil, e
			}
			value, e := calibrationNumber(vr)
			if e != nil {
				return nil, e
			}
			typRaw, e := get("type")
			if e != nil {
				return nil, e
			}
			typ, e := calibrationNumber(typRaw)
			if e != nil || typ != math.Trunc(typ) || typ < 0 || typ > 3 {
				return nil, fmt.Errorf("invalid history type")
			}
			if result[id] == nil {
				result[id] = make(map[int64]CalibrationHistory)
			}
			result[id][ts] = CalibrationHistory{value, int(typ)}
		}
	}
	return result, nil
}

func calibrationNumber(raw interface{}) (float64, error) {
	var v float64
	var err error
	switch n := raw.(type) {
	case json.Number:
		v, err = n.Float64()
	case string:
		v, err = strconv.ParseFloat(n, 64)
	case float64:
		v = n
	default:
		return 0, fmt.Errorf("invalid calibration number")
	}
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid calibration number")
	}
	return v, nil
}
