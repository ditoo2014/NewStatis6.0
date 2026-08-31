// Package services 封装 MySQL 与 InfluxDB 的业务数据访问和持久化操作。
package services

import (
	"reflect"
	"regexp"
	"strings"
	"time"

	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var tianmaLineNamePattern = regexp.MustCompile(`^[一二三四五六七八九十百千万0-9]+号线[_-]`)

// FindAccumulateById 判断统计器是否存在，并返回数据库记录是否与传入配置完全一致。
func FindAccumulateById(acc model.Accumulate) (bool, bool) {
	var data model.Accumulate
	common.GetMysql().Model(&model.Accumulate{}).Where("id = ?", acc.ID).First(&data)
	if data.ID == "" {
		return false, false
	}
	return true, reflect.DeepEqual(acc, data)
}

// CreateAccumulate 批量写入统计器基础配置。
func CreateAccumulate(list []model.Accumulate) error {
	tx := common.GetMysql().Begin()
	if tx.Error != nil {
		return tx.Error
	}
	if err := tx.Model(&model.Accumulate{}).Create(&list).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := syncTianmaAccumulatesTx(tx, list); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// UpdateAccumulate 在同一事务中更新统计器配置并删除其中间状态。
// 清除状态会使调度器按新配置重新建立计算游标，但不会删除既有历史统计点。
func UpdateAccumulate(stats []model.Accumulate) error {
	tx := common.GetMysql().Begin()
	for _, v := range stats {
		err := tx.Model(&model.Accumulate{}).Where("id = ?", v.ID).Updates(map[string]interface{}{
			"gatewayID": v.GatewayID, "point": v.Point, "statsName": v.StatsName, "type": v.Type,
			"startTime": v.StartTime, "deviceType": v.DeviceType, "count": v.Count, "notes": v.Notes, "largeName": v.LargeName,
		}).Error
		if err != nil {
			tx.Rollback()
			return err
		}
		if err = tx.Model(&model.AccStatsValue{}).Where("id = ?", v.ID).Delete(&model.AccStatsValue{}).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := syncTianmaAccumulatesTx(tx, stats); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// GetAllAccumulate 返回全部统计器配置；保留忽略错误的旧调用语义。
func GetAllAccumulate() (data []model.Accumulate) {
	data, _ = LoadAllAccumulate()
	return
}

// LoadAllAccumulate 返回全部统计器配置及数据库错误。
func LoadAllAccumulate() (data []model.Accumulate, err error) {
	err = common.GetMysql().Model(&model.Accumulate{}).Find(&data).Error
	return
}

// GetAccumulateList 分页查询全部类型的统计器，并支持多个展示字段模糊搜索。
func GetAccumulateList(pageInfo model.AccPageInfo) (data []model.Accumulate) {
	offset := (pageInfo.Page - 1) * pageInfo.Size
	likeQuery := "%" + pageInfo.Search + "%"
	query := common.GetMysql().Model(&model.Accumulate{})
	if pageInfo.Search != "" {
		query = query.Where("id LIKE ? OR statsName LIKE ? OR deviceType LIKE ? OR count LIKE ? OR notes LIKE ? OR largeName LIKE ?",
			likeQuery, likeQuery, likeQuery, likeQuery, likeQuery, likeQuery)
	}
	query.Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").Limit(pageInfo.Size).Offset(offset).Find(&data)
	return
}

// GetAccumulateList2 分页查询指定统计类型的统计器。
func GetAccumulateList2(pageInfo model.AccPageInfo, typeNum int) (data []model.Accumulate) {
	offset := (pageInfo.Page - 1) * pageInfo.Size
	likeQuery := "%" + pageInfo.Search + "%"
	query := common.GetMysql().Model(&model.Accumulate{}).Where("type = ?", typeNum)
	if pageInfo.Search != "" {
		query = query.Where("id LIKE ? OR statsName LIKE ? OR deviceType LIKE ? OR count LIKE ? OR notes LIKE ? OR largeName LIKE ?",
			likeQuery, likeQuery, likeQuery, likeQuery, likeQuery, likeQuery)
	}
	query.Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").Limit(pageInfo.Size).Offset(offset).Find(&data)
	return
}

// GetAccumulateTotal 统计全部类型中符合搜索条件的统计器数量。
func GetAccumulateTotal(pageInfo model.AccPageInfo) (total int64) {
	likeQuery := "%" + pageInfo.Search + "%"
	query := common.GetMysql().Model(&model.Accumulate{})
	if pageInfo.Search != "" {
		query = query.Where("id LIKE ? OR statsName LIKE ? OR deviceType LIKE ? OR count LIKE ? OR notes LIKE ? OR largeName LIKE ?",
			likeQuery, likeQuery, likeQuery, likeQuery, likeQuery, likeQuery)
	}
	query.Count(&total)
	return
}

// GetAccumulateTotal2 统计指定类型中符合搜索条件的统计器数量。
func GetAccumulateTotal2(pageInfo model.AccPageInfo, typeNum int) (total int64) {
	likeQuery := "%" + pageInfo.Search + "%"
	query := common.GetMysql().Model(&model.Accumulate{}).Where("type = ?", typeNum)
	if pageInfo.Search != "" {
		query = query.Where("id LIKE ? OR statsName LIKE ? OR deviceType LIKE ? OR count LIKE ? OR notes LIKE ? OR largeName LIKE ?",
			likeQuery, likeQuery, likeQuery, likeQuery, likeQuery, likeQuery)
	}
	query.Count(&total)
	return
}

// DelAccumulate 在事务中删除统计器配置及当前计算状态。
// 此兼容方法不负责清理 InfluxDB 历史，完整删除流程使用 DelAccumulateWithHistoryTasks。
func DelAccumulate(list model.IDList) error {
	tx := common.GetMysql().Begin()
	if err := tx.Model(&model.Accumulate{}).Where("id IN ?", list.List).Delete(&model.Accumulate{}).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Model(&model.AccStatsValue{}).Where("id IN ?", list.List).Delete(&model.AccStatsValue{}).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := deleteTianmaByIDsTx(tx, list.List); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// GetAccList 按类型返回统计器列表，并按 ID 中的数字部分排序。
func GetAccList(Type int) (dataList []model.Accumulate) {
	common.GetMysql().Model(&model.Accumulate{}).Where("type = ?", Type).Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").Find(&dataList)
	return
}

// SearchAccList 按统计类型及 ID、设备类型或关键字筛选报表所需统计器。
func SearchAccList(acc model.AccFormPage) (dataList []model.Accumulate) {
	query := common.GetMysql().Model(&model.Accumulate{}).Where("type = ?", acc.Type)
	if acc.IDAndName != "" {
		arr := strings.Split(acc.IDAndName, ":")
		query = query.Where("id = ?", arr[0])
	} else if acc.DeviceType != "" {
		query = query.Where("deviceType = ?", acc.DeviceType)
	} else if acc.Search != "" {
		likeQuery := "%" + acc.Search + "%"
		query = query.Where("id LIKE ? OR statsName LIKE ? OR deviceType LIKE ? OR notes LIKE ? OR count LIKE ?",
			likeQuery, likeQuery, likeQuery, likeQuery, likeQuery)
	}
	query.Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").Find(&dataList)
	return
}

// AccListByIDAndName 从“ID:名称”列表提取 ID，并查询同一类型的统计器配置。
func AccListByIDAndName(acc model.AccFormPageByIDAndName) (dataList []model.Accumulate) {
	var ids []string
	for _, v := range acc.IDAndNameList {
		ids = append(ids, strings.Split(v, ":")[0])
	}
	common.GetMysql().Model(&model.Accumulate{}).Where("type = ? AND id IN ?", acc.Type, ids).Find(&dataList)
	return
}

// AccGetIdAndName 返回前端选择器使用的“ID:名称”列表。
func AccGetIdAndName(ty int) (dataListStr []string) {
	var data []model.Accumulate
	common.GetMysql().Model(&model.Accumulate{}).Select("id,statsName").Where("type = ?", ty).Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").Find(&data)
	for _, v := range data {
		dataListStr = append(dataListStr, v.ID+":"+v.StatsName)
	}
	return
}

// 天马项目报表白名单表中的统计器配置。
// AccGetReportIdAndName 返回天马报表白名单表中的“ID:名称”列表。
func AccGetReportIdAndName(ty int, project string) (dataListStr []string) {
	if project != "tianma" {
		return
	}
	prefix, ok := accTypeIDPrefix(ty)
	if !ok {
		return
	}
	var data []model.Accumulate
	common.GetMysql().Table("tianma").
		Select("id,statsName").
		Where("type = ? AND id LIKE ?", ty, prefix+"%").
		Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").
		Find(&data)
	for _, v := range data {
		dataListStr = append(dataListStr, v.ID+":"+v.StatsName)
	}
	return
}

func accTypeIDPrefix(ty int) (string, bool) {
	switch ty {
	case 0:
		return "B", true
	case 1:
		return "H", true
	case 2:
		return "D", true
	case 3:
		return "M", true
	default:
		return "", false
	}
}

func syncTianmaAccumulatesTx(tx *gorm.DB, list []model.Accumulate) error {
	for _, acc := range list {
		if isTianmaReportAccumulate(acc) {
			if err := upsertTianmaAccumulateTx(tx.Table("tianma"), acc); err != nil {
				return err
			}
			continue
		}
		if strings.TrimSpace(acc.ID) != "" {
			if err := tx.Table("tianma").Where("id = ?", acc.ID).Delete(&model.Accumulate{}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func upsertTianmaAccumulateTx(db *gorm.DB, acc model.Accumulate) error {
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"gatewayID",
			"point",
			"statsName",
			"type",
			"startTime",
			"deviceType",
			"count",
			"notes",
			"largeName",
		}),
	}).Create(&acc).Error
}

func isTianmaReportAccumulate(acc model.Accumulate) bool {
	statsName := strings.TrimSpace(acc.StatsName)
	if !tianmaLineNamePattern.MatchString(statsName) {
		return false
	}
	prefix, ok := accTypeIDPrefix(acc.Type)
	if !ok {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(acc.ID), prefix)
}

func deleteTianmaByIDsTx(tx *gorm.DB, ids []string) error {
	var normalized []string
	seen := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		normalized = append(normalized, id)
	}
	if len(normalized) == 0 {
		return nil
	}
	return tx.Table("tianma").Where("id IN ?", normalized).Delete(&model.Accumulate{}).Error
}

// AccGetDevice 返回统计器配置中不重复的设备类型。
func AccGetDevice() (strList []string) {
	var data []model.Accumulate
	common.GetMysql().Model(&model.Accumulate{}).Select("distinct deviceType").Find(&data)
	for _, v := range data {
		strList = append(strList, v.DeviceType)
	}
	return
}

// GetAccListByIdList 按 ID 集合读取统计器配置。
func GetAccListByIdList(idList []string) (dataList []model.Accumulate) {
	common.GetMysql().Model(&model.Accumulate{}).Where("id IN ?", idList).Find(&dataList)
	return
}

// GetStatsByIds 按 ID 集合读取统计器，并保持与列表页面一致的排序方式。
func GetStatsByIds(ids []string) (data []model.Accumulate) {
	common.GetMysql().Model(&model.Accumulate{}).Where("id IN ?", ids).Order("CAST(SUBSTRING(id, 2) AS UNSIGNED)").Find(&data)
	return
}

// GetAccByCode 按统计池标签中的统计编码批量读取统计器配置。
func GetAccByCode(code []string) (data []model.Accumulate, err error) {
	result := common.GetMysql().Model(&model.Accumulate{}).Where("id IN ?", code).Find(&data)
	return data, result.Error
}

// GetAccStatsValue 返回单个统计器当前状态；保留忽略错误的旧调用语义。
func GetAccStatsValue(accId string) (data model.AccStatsValue) {
	data, _ = LoadAccStatsValue(accId)
	return
}

// LoadAccStatsValue 返回单个统计器当前计算状态及数据库错误。
func LoadAccStatsValue(accID string) (data model.AccStatsValue, err error) {
	err = common.GetMysql().Model(&model.AccStatsValue{}).Where("id = ?", accID).Limit(1).Find(&data).Error
	return
}

// GetAccStatsValueMap 批量返回以统计器 ID 为键的状态映射，并兼容忽略错误的旧调用方。
func GetAccStatsValueMap(ids []string) map[string]model.AccStatsValue {
	dataMap, _ := LoadAccStatsValueMap(ids)
	return dataMap
}

// LoadAccStatsValueMap 通过一次 MySQL 查询批量加载统计器状态，避免逐条查询。
func LoadAccStatsValueMap(ids []string) (map[string]model.AccStatsValue, error) {
	dataMap := make(map[string]model.AccStatsValue, len(ids))
	if len(ids) == 0 {
		return dataMap, nil
	}
	var data []model.AccStatsValue
	if err := common.GetMysql().Model(&model.AccStatsValue{}).Where("id IN ?", ids).Find(&data).Error; err != nil {
		return dataMap, err
	}
	for _, v := range data {
		dataMap[v.ID] = v
	}
	return dataMap, nil
}

// IsExistAccumulateStats 判断统计器是否已经建立当前计算状态。
func IsExistAccumulateStats(acc model.Accumulate) bool {
	return GetAccStatsValue(acc.ID).ID != ""
}

// SaveAccStatsValue 保存一个统计器状态，并复用批量 Upsert 规则。
func SaveAccStatsValue(value model.AccStatsValue) error {
	return SaveAccStatsValues([]model.AccStatsValue{value})
}

// SaveAccStatsValues 分批 Upsert 当前统计状态。
// 首次写入补齐创建时间；冲突更新时保留原创建时间并刷新业务游标和更新时间。
func SaveAccStatsValues(values []model.AccStatsValue) error {
	if len(values) == 0 {
		return nil
	}
	now := time.Now()
	if config.Location != nil {
		now = now.In(config.Location)
	}
	for i := range values {
		if values[i].CreatedAt.IsZero() {
			values[i].CreatedAt = now
		}
		values[i].UpdatedAt = now
	}
	batchSize := config.Conf.Stats.StateUpsertBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	return common.GetMysql().Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"type",
			"period_start_time",
			"period_end_time",
			"start_time",
			"start_value",
			"end_time",
			"end_value",
			"has_data",
			"gap",
			"updated_at",
		}),
	}).CreateInBatches(&values, batchSize).Error
}

// DeleteAccStatsValue 删除一个统计器的当前计算状态，并在失败时记录日志。
func DeleteAccStatsValue(id string) {
	if err := common.GetMysql().Where("id = ?", id).Delete(&model.AccStatsValue{}).Error; err != nil {
		logger.Error("delete middle record failed", err)
	}
}
