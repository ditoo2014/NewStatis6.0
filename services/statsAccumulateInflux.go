package services

import (
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"NewStatis6.0/utils"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

// PointValue 保存原始测点在某一采集时刻的累计值。
type PointValue struct {
	Time  int64
	Value float64
}

// rawMeasurement 根据统计器网关确定原始采集数据所在的 measurement。
func rawMeasurement(acc model.Accumulate) string {
	return acc.GatewayID
}

// validPointWhere 构造统计器测点、有效状态和非零值的既有过滤条件。
func validPointWhere(acc model.Accumulate) string {
	return fmt.Sprintf("\"id\"='%s' and status = 1 and (value != 0 or value != '0')", strings.ReplaceAll(acc.Point, "'", "\\'"))
}

// queryFirstPoint 执行单点 InfluxQL，并区分“查询失败”和“查询成功但没有数据”。
// 第二个返回值仅在成功解析到有效时间和值时为 true。
func queryFirstPoint(cmd string) (PointValue, bool, error) {
	res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	if err != nil {
		return PointValue{}, false, err
	}
	if res == nil {
		return PointValue{}, false, fmt.Errorf("influx query returned nil response")
	}
	if err = res.Error(); err != nil {
		return PointValue{}, false, err
	}
	if len(res.Results) == 0 || len(res.Results[0].Series) == 0 || len(res.Results[0].Series[0].Values) == 0 {
		return PointValue{}, false, nil
	}
	row := res.Results[0].Series[0].Values[0]
	ts, ok := parseInfluxTimestamp(row)
	if !ok {
		return PointValue{}, false, fmt.Errorf("invalid influx point timestamp")
	}
	value, ok := parseInfluxValue(row)
	if !ok {
		return PointValue{}, false, fmt.Errorf("invalid influx point value")
	}
	return PointValue{Time: ts, Value: value}, true, nil
}

// parseInfluxTimestamp 兼容 InfluxDB 客户端可能返回的多种时间戳类型。
func parseInfluxTimestamp(row []interface{}) (int64, bool) {
	if len(row) == 0 {
		return 0, false
	}
	switch v := row[0].(type) {
	case json.Number:
		ts, err := v.Int64()
		return ts, err == nil
	case float64:
		return int64(v), true
	case string:
		ts, err := strconv.ParseInt(v, 10, 64)
		return ts, err == nil
	default:
		return 0, false
	}
}

// parseInfluxValue 从原始查询或聚合查询的不同行布局中提取数值。
func parseInfluxValue(row []interface{}) (float64, bool) {
	if len(row) == 0 {
		return 0, false
	}
	var raw interface{}
	if len(row) >= 5 {
		raw = row[4]
	} else if len(row) >= 4 {
		raw = row[3]
	} else if len(row) >= 2 {
		raw = row[1]
	}
	switch v := raw.(type) {
	case json.Number:
		val, err := v.Float64()
		return val, err == nil
	case string:
		val, err := strconv.ParseFloat(v, 64)
		return val, err == nil
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// FindFirstPointInRange 查询左闭右开时间范围 [start, end) 内的第一个有效采集点。
func FindFirstPointInRange(acc model.Accumulate, start, end int64) (PointValue, bool, error) {
	cmd := fmt.Sprintf("SELECT * FROM \"%s\" WHERE %s and time >= %d000000000 and time < %d000000000 order by time asc limit 1",
		escapeInfluxIdentifier(rawMeasurement(acc)), validPointWhere(acc), start, end)
	return queryFirstPoint(cmd)
}

// FindLastPointInRange 查询左闭右开时间范围 [start, end) 内的最后一个有效采集点。
func FindLastPointInRange(acc model.Accumulate, start, end int64) (PointValue, bool, error) {
	cmd := fmt.Sprintf("SELECT * FROM \"%s\" WHERE %s and time >= %d000000000 and time < %d000000000 order by time desc limit 1",
		escapeInfluxIdentifier(rawMeasurement(acc)), validPointWhere(acc), start, end)
	return queryFirstPoint(cmd)
}

// FindFirstPointInPeriod 查询完整统计周期中的第一个有效采集点。
func FindFirstPointInPeriod(acc model.Accumulate, period utils.Period) (PointValue, bool, error) {
	return FindFirstPointInRange(acc, period.Start, period.End)
}

// FindLastPointInPeriod 查询完整统计周期中的最后一个有效采集点。
func FindLastPointInPeriod(acc model.Accumulate, period utils.Period) (PointValue, bool, error) {
	return FindLastPointInRange(acc, period.Start, period.End)
}

// FindLatestPointBefore 查询指定时刻之前最近的有效采集点，不包含该时刻。
func FindLatestPointBefore(acc model.Accumulate, timestamp int64) (PointValue, bool, error) {
	cmd := fmt.Sprintf("SELECT * FROM \"%s\" WHERE %s and time < %d000000000 order by time desc limit 1",
		escapeInfluxIdentifier(rawMeasurement(acc)), validPointWhere(acc), timestamp)
	return queryFirstPoint(cmd)
}

// FindFirstPointAfter 查询指定时刻之后最早的有效采集点，不包含该时刻。
func FindFirstPointAfter(acc model.Accumulate, timestamp int64) (PointValue, bool, error) {
	cmd := fmt.Sprintf("SELECT * FROM \"%s\" WHERE %s and time > %d000000000 order by time asc limit 1",
		escapeInfluxIdentifier(rawMeasurement(acc)), validPointWhere(acc), timestamp)
	return queryFirstPoint(cmd)
}

// FindBoundaryPointsInRange 批量查询同一 measurement 下多个测点的首值和末值。
// 测点 ID 会去重并按配置拆批，以控制 InfluxQL 长度和单次查询负载。
func FindBoundaryPointsInRange(accList []model.Accumulate, start, end int64) (map[string]PointValue, map[string]PointValue, error) {
	firstPoints := make(map[string]PointValue)
	lastPoints := make(map[string]PointValue)
	if len(accList) == 0 {
		return firstPoints, lastPoints, nil
	}
	measurement := rawMeasurement(accList[0])
	pointSeen := make(map[string]bool, len(accList))
	pointIDs := make([]string, 0, len(accList))
	for _, acc := range accList {
		if rawMeasurement(acc) != measurement {
			return nil, nil, fmt.Errorf("batch contains multiple raw measurements")
		}
		if acc.Point == "" || pointSeen[acc.Point] {
			continue
		}
		pointSeen[acc.Point] = true
		pointIDs = append(pointIDs, acc.Point)
	}
	batchSize := config.Conf.Stats.SourceIDBatchSize
	if batchSize <= 0 {
		batchSize = 300
	}
	for offset := 0; offset < len(pointIDs); offset += batchSize {
		limit := offset + batchSize
		if limit > len(pointIDs) {
			limit = len(pointIDs)
		}
		ids := pointIDs[offset:limit]
		firstBatch, err := queryGroupedSelector(measurement, ids, start, end, "first")
		if err != nil {
			return nil, nil, err
		}
		lastBatch, err := queryGroupedSelector(measurement, ids, start, end, "last")
		if err != nil {
			return nil, nil, err
		}
		for id, point := range firstBatch {
			firstPoints[id] = point
		}
		for id, point := range lastBatch {
			lastPoints[id] = point
		}
	}
	return firstPoints, lastPoints, nil
}

// queryGroupedSelector 使用 GROUP BY id 执行 first 或 last 聚合，并按测点返回结果。
func queryGroupedSelector(measurement string, pointIDs []string, start, end int64, selector string) (map[string]PointValue, error) {
	points := make(map[string]PointValue, len(pointIDs))
	if len(pointIDs) == 0 {
		return points, nil
	}
	if selector != "first" && selector != "last" {
		return nil, fmt.Errorf("unsupported influx selector: %s", selector)
	}
	filters := make([]string, 0, len(pointIDs))
	for _, id := range pointIDs {
		filters = append(filters, fmt.Sprintf("\"id\"='%s'", escapeInfluxString(id)))
	}
	cmd := fmt.Sprintf("SELECT %s(value) AS value FROM \"%s\" WHERE status = 1 and (value != 0 or value != '0') and time >= %d000000000 and time < %d000000000 and (%s) GROUP BY \"id\"",
		selector, escapeInfluxIdentifier(measurement), start, end, strings.Join(filters, " OR "))
	res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("influx batch query returned nil response")
	}
	if err = res.Error(); err != nil {
		return nil, err
	}
	if len(res.Results) == 0 {
		return points, nil
	}
	for _, series := range res.Results[0].Series {
		id := series.Tags["id"]
		if id == "" || len(series.Values) == 0 {
			continue
		}
		row := series.Values[0]
		ts, ok := parseInfluxTimestamp(row)
		if !ok {
			return nil, fmt.Errorf("invalid influx batch timestamp for point %s", id)
		}
		value, ok := parseInfluxValue(row)
		if !ok {
			return nil, fmt.Errorf("invalid influx batch value for point %s", id)
		}
		points[id] = PointValue{Time: ts, Value: value}
	}
	return points, nil
}

// WriteHistoryPoint 将一个已结束周期的统计值加入批量写入对象。
// 历史点时间固定为周期起点，负增量按配置转换为 0。
func WriteHistoryPoint(acc model.Accumulate, period utils.Period, value float64, bp client.BatchPoints) error {
	if value < 0 && config.Conf.Stats.NegativeDeltaPolicy == "zero" {
		logger.Warn("negative delta converted to zero", acc.ID)
		value = 0
	}
	value = utils.RoundStatsValue(value)
	tags := map[string]string{"id": acc.ID}
	fields := map[string]interface{}{"data": value, "type": acc.Type}
	pt, err := client.NewPoint(config.Conf.Influx.HistoryMeasurement, tags, fields, time.Unix(period.Start, 0).In(config.Location))
	if err != nil {
		return err
	}
	bp.AddPoint(pt)
	return nil
}

// HistoryPointExists 判断指定周期起点是否已经存在统计历史点。
// 该兼容方法将查询错误与不存在都返回为 false，新批处理流程使用带错误返回的方法。
func HistoryPointExists(id string, timestamp int64) bool {
	cmd := fmt.Sprintf("SELECT data FROM \"%s\" WHERE id = '%s' and time = %d000000000 limit 1",
		config.Conf.Influx.HistoryMeasurement, strings.ReplaceAll(id, "'", "\\'"), timestamp)
	res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	return err == nil && res.Error() == nil && len(res.Results) > 0 && len(res.Results[0].Series) > 0
}

// FindHistoryPointTimes 读取时间范围内已有历史点的时间集合，用于补算时避免覆盖生产数据。
func FindHistoryPointTimes(id string, start, end int64) (map[int64]bool, error) {
	times := make(map[int64]bool)
	if id == "" || start >= end {
		return times, nil
	}
	cmd := fmt.Sprintf("SELECT data FROM \"%s\" WHERE id = '%s' and time >= %d000000000 and time < %d000000000",
		escapeInfluxIdentifier(config.Conf.Influx.HistoryMeasurement), escapeInfluxString(id), start, end)
	res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("influx history timestamp query returned nil response")
	}
	if err = res.Error(); err != nil {
		return nil, err
	}
	if len(res.Results) == 0 {
		return times, nil
	}
	for _, series := range res.Results[0].Series {
		for _, row := range series.Values {
			ts, ok := parseInfluxTimestamp(row)
			if !ok {
				return nil, fmt.Errorf("invalid history timestamp for stats id=%s", id)
			}
			times[ts] = true
		}
	}
	return times, nil
}

// FindExistingHistoryIDsAtTime 分批查询指定周期起点已有历史数据的统计器 ID。
func FindExistingHistoryIDsAtTime(ids []string, timestamp int64) (map[string]bool, error) {
	existing := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return existing, nil
	}
	batchSize := config.Conf.Stats.SourceIDBatchSize
	if batchSize <= 0 {
		batchSize = 300
	}
	for offset := 0; offset < len(ids); offset += batchSize {
		limit := offset + batchSize
		if limit > len(ids) {
			limit = len(ids)
		}
		filters := make([]string, 0, limit-offset)
		for _, id := range ids[offset:limit] {
			filters = append(filters, fmt.Sprintf("id = '%s'", escapeInfluxString(id)))
		}
		cmd := fmt.Sprintf("SELECT last(data) FROM \"%s\" WHERE time = %d000000000 and (%s) GROUP BY id",
			escapeInfluxIdentifier(config.Conf.Influx.HistoryMeasurement), timestamp, strings.Join(filters, " OR "))
		res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, fmt.Errorf("influx existing history query returned nil response")
		}
		if err = res.Error(); err != nil {
			return nil, err
		}
		if len(res.Results) == 0 {
			continue
		}
		for _, series := range res.Results[0].Series {
			if id := series.Tags["id"]; id != "" && len(series.Values) > 0 {
				existing[id] = true
			}
		}
	}
	return existing, nil
}

// DeleteHistoryByIDs 逐个删除统计器历史结果并记录失败，不删除原始采集数据。
func DeleteHistoryByIDs(ids []string) {
	for _, id := range ids {
		if err := DeleteHistoryByID(id, config.Conf.Influx.HistoryMeasurement); err != nil {
			logger.Error("delete history failed", err)
		}
	}
}

// SelectAccFormInfo 按可选时间范围查询单个统计器的全部历史报表数据。
func SelectAccFormInfo(accFormPage model.AccFormPage, acc model.Accumulate) []model.AccForm {
	var cmd string
	if accFormPage.StartTime == 0 && accFormPage.EndTime == 0 {
		cmd = fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE id = '%s' order by time desc", config.Conf.Influx.HistoryMeasurement, acc.ID)
	} else {
		cmd = fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE time >= %d000000000 and time <= %d000000000 and id = '%s' order by time desc",
			config.Conf.Influx.HistoryMeasurement, accFormPage.StartTime, accFormPage.EndTime, acc.ID)
	}
	return queryAccForms(cmd, accFormPage.Type, acc)
}

// SelectAccFormInfoDefault 执行页面默认查询：非月统计取最近一个月，月统计保留全部历史。
func SelectAccFormInfoDefault(accFormPage model.AccFormPage, acc model.Accumulate) []model.AccForm {
	accFormPage.StartTime = utils.SanMonthTime()
	var cmd string
	if acc.Type == 3 {
		cmd = fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE id = '%s' order by time desc", config.Conf.Influx.HistoryMeasurement, acc.ID)
	} else {
		cmd = fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE time >= %d000000000 and id = '%s' order by time desc",
			config.Conf.Influx.HistoryMeasurement, accFormPage.StartTime, acc.ID)
	}
	return queryAccForms(cmd, acc.Type, acc)
}

// SelectAccFormInfoPage 分页查询历史并保留忽略错误的旧调用语义。
func SelectAccFormInfoPage(accFormPage model.AccFormPage, acc model.Accumulate, defaultQuery bool, offset, limit int) []model.AccForm {
	forms, _ := SelectAccFormInfoPageE(accFormPage, acc, defaultQuery, offset, limit)
	return forms
}

// SelectAccFormInfoPageE 在 InfluxDB 端通过 limit/offset 分页查询历史，并返回查询错误。
func SelectAccFormInfoPageE(accFormPage model.AccFormPage, acc model.Accumulate, defaultQuery bool, offset, limit int) ([]model.AccForm, error) {
	if limit <= 0 {
		return []model.AccForm{}, nil
	}
	where := buildAccHistoryWhere(accFormPage, acc, defaultQuery)
	cmd := fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE %s order by time desc limit %d offset %d",
		config.Conf.Influx.HistoryMeasurement, where, limit, offset)
	return queryAccFormsE(cmd, acc.Type, acc)
}

// CountAccFormInfoMap 批量统计多个统计器的历史条数，结果映射包含无数据统计器的零值。
func CountAccFormInfoMap(accFormPage model.AccFormPage, accList []model.Accumulate, defaultQuery bool) (map[string]int, error) {
	result := make(map[string]int, len(accList))
	if len(accList) == 0 {
		return result, nil
	}
	ids := make([]string, 0, len(accList))
	for _, acc := range accList {
		result[acc.ID] = 0
		ids = append(ids, acc.ID)
	}
	batchSize := config.Conf.Stats.SourceIDBatchSize
	if batchSize <= 0 {
		batchSize = 300
	}
	for start := 0; start < len(ids); start += batchSize {
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		where := buildAccHistoryBatchWhere(accFormPage, accList[0].Type, ids[start:end], defaultQuery)
		if where == "" {
			continue
		}
		cmd := fmt.Sprintf("SELECT count(data) FROM \"%s\" WHERE %s GROUP BY id", config.Conf.Influx.HistoryMeasurement, where)
		res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
		if err != nil {
			return result, err
		}
		if res == nil {
			return result, fmt.Errorf("influx count query returned nil response")
		}
		if err = res.Error(); err != nil {
			return result, err
		}
		if len(res.Results) == 0 || len(res.Results[0].Series) == 0 {
			continue
		}
		for _, series := range res.Results[0].Series {
			id := series.Tags["id"]
			if id == "" || len(series.Values) == 0 || len(series.Values[0]) < 2 {
				continue
			}
			result[id] = parseInfluxCount(series.Values[0][1])
		}
	}
	return result, nil
}

// SelectAccFormInfoBatch 为兼容旧调用方，按统计器汇总各自的历史查询结果。
func SelectAccFormInfoBatch(accFormPage model.AccFormPage, accList []model.Accumulate) map[string][]model.AccForm {
	result := make(map[string][]model.AccForm, len(accList))
	for _, acc := range accList {
		result[acc.ID] = SelectAccFormInfo(accFormPage, acc)
	}
	return result
}

// buildAccHistoryWhere 组合单个统计器的时间条件和 ID 条件。
func buildAccHistoryWhere(accFormPage model.AccFormPage, acc model.Accumulate, defaultQuery bool) string {
	parts := make([]string, 0, 2)
	if timeWhere := buildAccHistoryTimeWhere(accFormPage, acc.Type, defaultQuery); timeWhere != "" {
		parts = append(parts, timeWhere)
	}
	parts = append(parts, fmt.Sprintf("id = '%s'", escapeInfluxString(acc.ID)))
	return strings.Join(parts, " and ")
}

// buildAccHistoryBatchWhere 组合一批统计器共用的时间条件和 ID 条件。
func buildAccHistoryBatchWhere(accFormPage model.AccFormPage, accType int, ids []string, defaultQuery bool) string {
	idFilter := buildInfluxIDFilter(ids)
	if idFilter == "" {
		return ""
	}
	parts := make([]string, 0, 2)
	if timeWhere := buildAccHistoryTimeWhere(accFormPage, accType, defaultQuery); timeWhere != "" {
		parts = append(parts, timeWhere)
	}
	parts = append(parts, "("+idFilter+")")
	return strings.Join(parts, " and ")
}

// buildAccHistoryTimeWhere 根据显式范围或页面默认规则生成历史时间条件。
func buildAccHistoryTimeWhere(accFormPage model.AccFormPage, accType int, defaultQuery bool) string {
	if defaultQuery {
		if accType == 3 {
			return ""
		}
		return fmt.Sprintf("time >= %d000000000", utils.SanMonthTime())
	}
	if accFormPage.StartTime == 0 && accFormPage.EndTime == 0 {
		return ""
	}
	return fmt.Sprintf("time >= %d000000000 and time <= %d000000000", accFormPage.StartTime, accFormPage.EndTime)
}

// buildInfluxIDFilter 将统计器 ID 集合拼接为已转义的 InfluxQL OR 条件。
func buildInfluxIDFilter(ids []string) string {
	conditions := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		conditions = append(conditions, fmt.Sprintf("id = '%s'", escapeInfluxString(id)))
	}
	return strings.Join(conditions, " OR ")
}

// escapeInfluxString 转义单引号包裹的 InfluxQL 字符串值。
func escapeInfluxString(value string) string {
	return strings.ReplaceAll(value, "'", "\\'")
}

// parseInfluxCount 兼容 InfluxDB count 聚合结果可能使用的数值类型。
func parseInfluxCount(raw interface{}) int {
	switch v := raw.(type) {
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return int(n)
		}
		f, err := v.Float64()
		if err == nil {
			return int(f)
		}
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return 0
}

// queryAccForms 执行报表查询并保留忽略错误的旧调用语义。
func queryAccForms(cmd string, typ int, acc model.Accumulate) []model.AccForm {
	forms, _ := queryAccFormsE(cmd, typ, acc)
	return forms
}

// queryAccFormsE 将 InfluxDB 历史行转换为接口结构，并应用统计器配置的换算公式。
func queryAccFormsE(cmd string, typ int, acc model.Accumulate) ([]model.AccForm, error) {
	res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("influx history query returned nil response")
	}
	if err = res.Error(); err != nil {
		return nil, err
	}
	if len(res.Results) == 0 || len(res.Results[0].Series) == 0 {
		return nil, nil
	}
	var forms []model.AccForm
	for _, row := range res.Results[0].Series[0].Values {
		ts, _ := row[0].(json.Number).Int64()
		raw, _ := row[1].(json.Number).Float64()
		value, err := utils.ApplyFormula(acc.Count, raw)
		if err != nil {
			logger.Error("formula failed", err)
			value = 0
		}
		forms = append(forms, model.AccForm{
			ID:         acc.ID,
			Type:       acc.Type,
			Time:       utils.FormatPeriodTime(typ, ts),
			StatsName:  acc.StatsName,
			DeviceType: acc.DeviceType,
			Value:      utils.RoundStatsValue(value),
			Notes:      acc.Notes,
		})
	}
	return forms, nil
}

// FindStatsHistoryByTime 查询统计池标签关联统计器在指定范围内的历史结果。
func FindStatsHistoryByTime(req model.HistoryReq, label model.StatsDataPoolInfo, accStatsValue model.AccStatsValue) (dataList []model.RealDataRes) {
	cmd := fmt.Sprintf("SELECT data,type FROM \"%s\" WHERE id = '%s' and time >= %d000000000 and time <= %d000000000",
		config.Conf.Influx.HistoryMeasurement, label.StatsCode, req.StartTime, req.EndTime)
	res, err := common.GetInfluxDB().Query(client.NewQuery(cmd, config.Conf.Influx.Database, "s"))
	if err != nil || res.Error() != nil || len(res.Results) == 0 || len(res.Results[0].Series) == 0 {
		return
	}
	for _, row := range res.Results[0].Series[0].Values {
		ts, _ := row[0].(json.Number).Int64()
		value, _ := row[1].(json.Number).Float64()
		dataList = append(dataList, model.RealDataRes{
			LabelID:   label.LabelID,
			StatsCode: label.StatsCode,
			Value:     utils.RoundStatsValue(value),
			StatsName: label.StatsName,
			LabelName: label.LabelName,
			Time:      utils.FormatPeriodTime(accStatsValue.Type, ts),
			StatsType: StatsTypeName(accStatsValue.Type),
		})
	}
	return
}

// StatsTypeName 将统计类型编号转换为对外接口沿用的中文名称。
func StatsTypeName(typ int) string {
	switch typ {
	case 0:
		return "半小时增量统计"
	case 1:
		return "小时增量统计"
	case 2:
		return "日增量统计"
	case 3:
		return "月增量统计"
	default:
		return ""
	}
}
