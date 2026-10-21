package services

import (
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/model"
	"NewStatis6.0/utils"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var poolCodePrefixes = [...]string{"B", "H", "D", "M"}

func normalizePoolPoints(req model.CreateLabelsFromPointsReq) (model.CreateLabelsFromPointsReq, error) {
	req.GatewayID = strings.TrimSpace(req.GatewayID)
	if req.PoolID == 0 || req.GatewayID == "" || utf8.RuneCountInString(req.GatewayID) > 100 || len(req.Points) == 0 || len(req.Points) > 1000 {
		return req, errors.New("池、网关和数据点不能为空，每次最多选择1000个数据点")
	}
	seen := map[string]string{}
	points := make([]model.PoolPointInput, 0, len(req.Points))
	for _, p := range req.Points {
		p.DataID, p.Name = strings.TrimSpace(p.DataID), strings.TrimSpace(p.Name)
		if p.DataID == "" || p.Name == "" || utf8.RuneCountInString(p.DataID) > 100 {
			return req, errors.New("数据点ID和名称不能为空，数据点ID最多100个字符")
		}
		if name, exists := seen[p.DataID]; exists {
			if name != p.Name {
				return req, errors.New("同一数据点ID对应多个名称")
			}
			continue
		}
		seen[p.DataID] = p.Name
		points = append(points, p)
	}
	req.Points = points
	return req, nil
}

// CreateLabelsFromPoints commits all four types and their labels together.
// Lock order is sequence rows (0..3), then pool; code collisions only fail inserts,
// never route through the legacy create-or-update endpoint.
func CreateLabelsFromPoints(req model.CreateLabelsFromPointsReq) (model.CreatePoolLabelsResult, error) {
	result := model.CreatePoolLabelsResult{SkippedPoints: []string{}}
	req, err := normalizePoolPoints(req)
	if err != nil {
		return result, err
	}
	err = common.GetMysql().Transaction(func(tx *gorm.DB) error {
		var sequences []model.StatsCodeSequence
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Order("type").Find(&sequences).Error; err != nil {
			return fmt.Errorf("编号表读取失败，请检查服务启动日志及数据库建表权限: %w", err)
		}
		if len(sequences) != 4 {
			return errors.New("统计编号表记录不完整，请重启统计服务自动补齐")
		}
		for typ, seq := range sequences {
			if seq.Type != typ || seq.NextValue == 0 {
				return errors.New("统计编号表配置错误")
			}
		}
		var pool model.StatsDataPool
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pool, req.PoolID).Error; err != nil {
			return fmt.Errorf("统计池不存在或读取失败: %w", err)
		}
		var existing []struct {
			Point string
			Type  int
		}
		if err := tx.Table("stats_data_pool_info AS l").Select("a.point, a.type").
			Joins("JOIN accumulate AS a ON a.id = l.statsCode").
			Where("l.poolID = ? AND a.gatewayID = ?", pool.ID, req.GatewayID).Scan(&existing).Error; err != nil {
			return err
		}
		bound := map[string]bool{}
		for _, row := range existing {
			bound[poolPointKey(row.Point, row.Type)] = true
		}
		var xh uint
		if err := tx.Model(&model.StatsDataPoolInfo{}).Where("poolID = ?", pool.ID).
			Select("COALESCE(MAX(xh), 0)").Scan(&xh).Error; err != nil {
			return err
		}
		location := config.Location
		if location == nil {
			location, err = time.LoadLocation("Asia/Shanghai")
			if err != nil {
				return err
			}
		}
		start := poolMonthStart(time.Now(), location)
		var accumulates []model.Accumulate
		var labels []model.StatsDataPoolInfo
		usedLabelIDs := map[string]bool{}
		skippedPoints := map[string]bool{}
		for typ, prefix := range poolCodePrefixes {
			next := sequences[typ].NextValue
			// Include manually assigned IDs, including codes retained by orphan labels.
			var codes []string
			if err := tx.Model(&model.Accumulate{}).Where("id LIKE ?", prefix+"%").Pluck("id", &codes).Error; err != nil {
				return err
			}
			var labelCodes []string
			if err := tx.Model(&model.StatsDataPoolInfo{}).Where("statsCode LIKE ?", prefix+"%").Pluck("statsCode", &labelCodes).Error; err != nil {
				return err
			}
			for _, code := range append(codes, labelCodes...) {
				if n, ok := numericPoolCode(code, prefix); ok && n >= next {
					if n == ^uint64(0) {
						return errors.New("统计编号已耗尽")
					}
					next = n + 1
				}
			}
			for _, point := range req.Points {
				if bound[poolPointKey(point.DataID, typ)] {
					result.Skipped++
					if !skippedPoints[point.DataID] {
						result.SkippedPoints = append(result.SkippedPoints, point.DataID)
						skippedPoints[point.DataID] = true
					}
					continue
				}
				name := pool.PoolName + "-" + point.Name
				notes := point.Name + ";" + pool.PoolName
				if utf8.RuneCountInString(name) > 100 || utf8.RuneCountInString(notes) > 200 {
					return errors.New("池名称与数据点名称组合后过长，统计器名称最多100个字符")
				}
				if next == ^uint64(0) || xh >= uint(1<<31-1) {
					return errors.New("统计编号或序号已耗尽")
				}
				code := prefix + strconv.FormatUint(next, 10)
				next++
				xh++
				id, err := newPoolLabelID(tx, usedLabelIDs)
				if err != nil {
					return err
				}
				accumulates = append(accumulates, model.Accumulate{ID: code, StatsName: name, GatewayID: req.GatewayID,
					Point: point.DataID, Type: typ, StartTime: start, DeviceType: "能源-电表", Notes: notes})
				labels = append(labels, model.StatsDataPoolInfo{PoolID: pool.ID, XH: xh, LabelID: id,
					LabelName: name, StatsCode: code, StatsName: name})
			}
			if err := tx.Model(&model.StatsCodeSequence{}).Where("type = ?", typ).Update("next_value", next).Error; err != nil {
				return err
			}
		}
		if len(labels) == 0 {
			return nil
		}
		if err := tx.CreateInBatches(&accumulates, 200).Error; err != nil {
			return err
		}
		if err := tx.CreateInBatches(&labels, 200).Error; err != nil {
			return err
		}
		result.Created = len(labels)
		return nil
	})
	if err != nil {
		return model.CreatePoolLabelsResult{}, err
	}
	return result, nil
}

func poolPointKey(point string, typ int) string { return strconv.Itoa(typ) + ":" + point }

// poolMonthStart returns a full timestamp accepted by utils.Timestamp.
func poolMonthStart(now time.Time, location *time.Location) string {
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location).Format("2006-01-02 15:04:05")
}

func numericPoolCode(code, prefix string) (uint64, bool) {
	if !strings.HasPrefix(code, prefix) || len(code) <= len(prefix) {
		return 0, false
	}
	suffix := code[len(prefix):]
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(suffix, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func newPoolLabelID(tx *gorm.DB, used map[string]bool) (string, error) {
	for attempt := 0; attempt < 5; attempt++ {
		id, err := utils.GenerateRandomNumericString()
		if err != nil {
			return "", err
		}
		if used[id] {
			continue
		}
		var count int64
		if err := tx.Model(&model.StatsDataPoolInfo{}).Where("labelID = ?", id).Count(&count).Error; err != nil {
			return "", err
		}
		if count == 0 {
			used[id] = true
			return id, nil
		}
	}
	return "", errors.New("标签ID生成失败，请重试")
}

// CreateLegacyPoolLabels keeps the old request format, with pool-scoped deduplication.
func CreateLegacyPoolLabels(labels []model.StatsDataPoolInfo) (int, []string, error) {
	if len(labels) == 0 || labels[0].PoolID == 0 {
		return 0, nil, errors.New("标签和统计池不能为空")
	}
	poolID := labels[0].PoolID
	for _, label := range labels {
		if label.PoolID != poolID || strings.TrimSpace(label.StatsCode) == "" || strings.TrimSpace(label.LabelName) == "" {
			return 0, nil, errors.New("标签必须属于同一统计池，名称和统计器编号不能为空")
		}
	}
	created := 0
	var skipped []string
	err := common.GetMysql().Transaction(func(tx *gorm.DB) error {
		var pool model.StatsDataPool
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pool, poolID).Error; err != nil {
			return err
		}
		var existing []model.StatsDataPoolInfo
		if err := tx.Where("poolID = ?", poolID).Find(&existing).Error; err != nil {
			return err
		}
		seen := map[string]bool{}
		var xh uint
		for _, label := range existing {
			seen[label.StatsCode] = true
			if label.XH > xh {
				xh = label.XH
			}
		}
		usedIDs := map[string]bool{}
		var inserts []model.StatsDataPoolInfo
		for _, label := range labels {
			if seen[label.StatsCode] {
				skipped = append(skipped, label.StatsCode)
				continue
			}
			var acc model.Accumulate
			if err := tx.First(&acc, "id = ?", label.StatsCode).Error; err != nil {
				return fmt.Errorf("统计器不存在: %w", err)
			}
			id, err := newPoolLabelID(tx, usedIDs)
			if err != nil {
				return err
			}
			if xh >= uint(1<<31-1) {
				return errors.New("标签序号已耗尽")
			}
			xh++
			label.ID, label.XH, label.LabelID = 0, xh, id
			inserts = append(inserts, label)
			seen[label.StatsCode] = true
		}
		if len(inserts) > 0 {
			if err := tx.CreateInBatches(&inserts, 200).Error; err != nil {
				return err
			}
		}
		created = len(inserts)
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return created, skipped, nil
}
