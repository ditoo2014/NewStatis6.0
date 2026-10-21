package services

import (
	"NewStatis6.0/common"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"NewStatis6.0/utils"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
)

// GetPoolByName 判断统计池名称是否已存在。
func GetPoolByName(name string) bool {
	var data model.StatsDataPool
	common.GetMysql().Model(&model.StatsDataPool{}).Where("poolName = ?", name).First(&data)
	return data.PoolName != ""
}

// CreateDataPool 新建统计池配置。
func CreateDataPool(pool *model.StatsDataPool) error {
	pool.PoolName, pool.Admin = strings.TrimSpace(pool.PoolName), strings.TrimSpace(pool.Admin)
	if pool.PoolName == "" || pool.Admin == "" || utf8.RuneCountInString(pool.PoolName) > 255 || utf8.RuneCountInString(pool.Admin) > 100 {
		return errors.New("池名称和管理员不能为空或超出长度限制")
	}
	for attempt := 0; attempt < 5; attempt++ {
		var count int64
		if err := common.GetMysql().Model(&model.StatsDataPool{}).Where("poolName = ?", pool.PoolName).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return errors.New("名称重复")
		}
		key, err := utils.GenerateUniqueID()
		if err != nil {
			return err
		}
		pool.ID, pool.ApiKey = 0, key
		err = common.GetMysql().Create(pool).Error
		var duplicate *mysql.MySQLError
		if errors.As(err, &duplicate) && duplicate.Number == 1062 {
			continue
		}
		return err
	}
	return errors.New("名称重复或API Key冲突，请重试")
}

// UpdatePool 更新统计池名称和所属管理员，API Key 保持不变。
func UpdatePool(pool model.StatsDataPool) error {
	return common.GetMysql().Model(&model.StatsDataPool{}).Where("id = ?", pool.ID).Updates(map[string]interface{}{"poolName": pool.PoolName, "admin": pool.Admin}).Error
}

// DitooFindPoolDataByPageInfo 分页查询所有管理员的统计池。
func DitooFindPoolDataByPageInfo(pageInfo model.PageInfo) (data []model.StatsDataPool) {
	offset := (pageInfo.Page - 1) * pageInfo.Size
	query := common.GetMysql().Model(&model.StatsDataPool{})
	if pageInfo.Search != "" {
		query = query.Where("poolName LIKE ?", "%"+pageInfo.Search+"%")
	}
	query.Order("id asc").Limit(pageInfo.Size).Offset(offset).Find(&data)
	return
}

// DitooFindPoolDataTool 统计所有管理员范围内符合搜索条件的统计池数量。
func DitooFindPoolDataTool(pageInfo model.PageInfo) (total int64) {
	query := common.GetMysql().Model(&model.StatsDataPool{})
	if pageInfo.Search != "" {
		query = query.Where("poolName LIKE ?", "%"+pageInfo.Search+"%")
	}
	query.Count(&total)
	return
}

// FindPoolDataByPageInfo 分页查询指定管理员拥有的统计池。
func FindPoolDataByPageInfo(pageInfo model.PageInfo) (data []model.StatsDataPool) {
	offset := (pageInfo.Page - 1) * pageInfo.Size
	query := common.GetMysql().Model(&model.StatsDataPool{}).Where("admin = ?", pageInfo.Admin)
	if pageInfo.Search != "" {
		query = query.Where("poolName LIKE ?", "%"+pageInfo.Search+"%")
	}
	query.Order("id asc").Limit(pageInfo.Size).Offset(offset).Find(&data)
	return
}

// FindPoolDataTool 统计指定管理员范围内符合搜索条件的统计池数量。
func FindPoolDataTool(pageInfo model.PageInfo) (total int64) {
	query := common.GetMysql().Model(&model.StatsDataPool{}).Where("admin = ?", pageInfo.Admin)
	if pageInfo.Search != "" {
		query = query.Where("poolName LIKE ?", "%"+pageInfo.Search+"%")
	}
	query.Count(&total)
	return
}

// DelPool 在同一事务中删除统计池及其全部标签关系。
func DelPool(ids []int) error {
	if len(ids) == 0 {
		return errors.New("empty ids")
	}
	tx := common.GetMysql().Begin()
	if err := tx.Model(&model.StatsDataPool{}).Where("id IN ?", ids).Delete(&model.StatsDataPool{}).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Model(&model.StatsDataPoolInfo{}).Where("poolID IN ?", ids).Delete(&model.StatsDataPoolInfo{}).Error; err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

// CreateLabels 批量创建统计池标签关系。
func CreateLabels(list []model.StatsDataPoolInfo) error {
	return common.GetMysql().Model(&model.StatsDataPoolInfo{}).Create(&list).Error
}

// GetLabel 按显示序号返回指定统计池的标签列表。
func GetLabel(poolId int) (data []model.StatsDataPoolInfo, err error) {
	result := common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("poolID = ?", poolId).Order("xh").Find(&data)
	return data, result.Error
}

// FindLabelsByApiKey 验证 API Key 后读取标签，并可按统计编码首字母筛选统计类型。
func FindLabelsByApiKey(apiKey string, statsTypes []string) (data []model.StatsDataPoolInfo, err error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return data, errors.New("empty apiKey")
	}
	var pool model.StatsDataPool
	if err = common.GetMysql().Model(&model.StatsDataPool{}).Where("apiKey = ?", apiKey).First(&pool).Error; err != nil {
		return
	}
	var labels []model.StatsDataPoolInfo
	if err = common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("poolID = ?", pool.ID).Order("xh").Find(&labels).Error; err != nil {
		return
	}
	statsTypeMap := normalizeStatsTypes(statsTypes)
	if len(statsTypeMap) == 0 {
		return labels, nil
	}
	for _, label := range labels {
		statsCode := strings.TrimSpace(label.StatsCode)
		if statsCode == "" {
			continue
		}
		statsType := strings.ToUpper(statsCode[:1])
		if statsTypeMap[statsType] {
			data = append(data, label)
		}
	}
	return data, nil
}

// normalizeStatsTypes 将调用方传入的类型统一为大写首字母集合。
func normalizeStatsTypes(statsTypes []string) map[string]bool {
	statsTypeMap := map[string]bool{}
	for _, statsType := range statsTypes {
		statsType = strings.ToUpper(strings.TrimSpace(statsType))
		if statsType == "" {
			continue
		}
		statsTypeMap[statsType[:1]] = true
	}
	return statsTypeMap
}

// UpdateLabel 更新标签名称及其关联统计器信息。
func UpdateLabel(label model.StatsDataPoolInfo) error {
	return common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("id = ?", label.ID).Updates(map[string]interface{}{"labelName": label.LabelName, "statsName": label.StatsName, "statsCode": label.StatsCode}).Error
}

// DelLabelsById 按数据库主键批量删除统计池标签。
func DelLabelsById(ids []uint) error {
	return common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("id IN ?", ids).Delete(&model.StatsDataPoolInfo{}).Error
}

// GetNewLabelXH 返回统计池当前最大显示序号对应的标签。
func GetNewLabelXH(poolID uint) (data model.StatsDataPoolInfo, err error) {
	result := common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("poolID = ?", poolID).Order("xh desc").Limit(1).First(&data)
	return data, result.Error
}

// IsExistLabel 查询已经被标签引用的统计器编码。
func IsExistLabel(code []string) ([]model.StatsDataPoolInfo, error) {
	var data []model.StatsDataPoolInfo
	result := common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("statsCode IN ?", code).Find(&data)
	return data, result.Error
}

// FindLabelListByLabelIds 通过 API Key 定位统计池，并按可选标签 ID 集合读取标签。
func FindLabelListByLabelIds(apiKey string, labelIds []string) (data []model.StatsDataPoolInfo, err error) {
	var pool model.StatsDataPool
	if err = common.GetMysql().Model(&model.StatsDataPool{}).Where("apiKey = ?", apiKey).First(&pool).Error; err != nil {
		return
	}
	query := common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("poolID = ?", pool.ID)
	if len(labelIds) != 0 {
		query = query.Where("labelID IN ?", labelIds)
	}
	err = query.Find(&data).Error
	return
}

// FindLabelListByApiKeyAndLabelID 保留旧方法名并转发到统一标签查询实现。
func FindLabelListByApiKeyAndLabelID(apiKey string, labels []string) (data []model.StatsDataPoolInfo, err error) {
	return FindLabelListByLabelIds(apiKey, labels)
}

// FindAccRealDataByLabelID 批量读取标签关联统计器的当前周期状态。
func FindAccRealDataByLabelID(code []string) (data []model.AccStatsValue) {
	if err := common.GetMysql().Model(&model.AccStatsValue{}).Where("id IN ?", code).Find(&data).Error; err != nil {
		logger.Error("FindAccRealDataByLabelID failed", err)
	}
	return
}

// FindLabelsByIds 按主键集合读取标签并按显示序号排序。
func FindLabelsByIds(ids []int) (data []model.StatsDataPoolInfo, err error) {
	result := common.GetMysql().Model(&model.StatsDataPoolInfo{}).Where("id IN ?", ids).Order("xh").Find(&data)
	return data, result.Error
}

// BatchGetLabel 按 API Key 集合批量读取统计池基础信息。
func BatchGetLabel(apiKeyList []string) (data []model.StatsDataPool, err error) {
	result := common.GetMysql().Model(&model.StatsDataPool{}).Where("apikey IN ?", apiKeyList).Find(&data)
	return data, result.Error
}
