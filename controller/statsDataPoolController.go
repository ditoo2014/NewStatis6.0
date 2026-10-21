package controller

import (
	"NewStatis6.0/algorithm"
	"NewStatis6.0/model"
	"NewStatis6.0/services"
	"NewStatis6.0/utils"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xuri/excelize/v2"
)

// CreatePool 校验名称后创建带随机 API Key 的统计池。
func CreatePool(ctx *gin.Context) {
	var pool model.StatsDataPool
	if err := ctx.BindJSON(&pool); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	// Ignore client-supplied IDs and API keys.
	pool.ID, pool.ApiKey = 0, ""
	if err := services.CreateDataPool(&pool); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "创建成功", "data": pool})
}

// CreateLabelsFromPoints refreshes the scheduler only after a successful commit.
func CreateLabelsFromPoints(ctx *gin.Context) {
	var req model.CreateLabelsFromPointsReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	result, err := services.CreateLabelsFromPoints(req)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if result.Created > 0 {
		algorithm.GetAllAccumulate()
		algorithm.NotifyAccumulates()
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": fmt.Sprintf("新增%d个标签，跳过%d个已有标签", result.Created, result.Skipped), "data": result})
}

// UpdatePool 更新统计池名称和管理员信息。
func UpdatePool(ctx *gin.Context) {
	var pool model.StatsDataPool
	if err := ctx.BindJSON(&pool); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	if err := services.UpdatePool(pool); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "修改失败", "error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "修改成功"})
}

// GetPool 分页查询统计池；ditoo 管理员可查看全部，其余管理员只查看自己的数据。
func GetPool(ctx *gin.Context) {
	var page model.PageInfo
	if err := ctx.BindJSON(&page); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	if page.Page == 0 {
		page.Page = 1
	}
	if page.Size == 0 {
		page.Size = 10
	}
	if page.Admin == "" {
		page.Admin = "ditoo"
	}
	if page.Admin == "ditoo" {
		ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": services.DitooFindPoolDataByPageInfo(page), "total": services.DitooFindPoolDataTool(page)})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": services.FindPoolDataByPageInfo(page), "total": services.FindPoolDataTool(page)})
}

// DelPool 批量删除统计池及其标签关系。
func DelPool(ctx *gin.Context) {
	var ids []int
	if err := ctx.BindJSON(&ids); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	if err := services.DelPool(ids); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "删除失败", "error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "删除成功"})
}

// CreateLabel 为统计池批量创建标签，生成标签 ID 并接续现有显示序号。
// 保留旧请求格式，同池内已引用的统计编码跳过并在响应中提示。
func CreateLabel(ctx *gin.Context) {
	var labels []model.StatsDataPoolInfo
	if err := ctx.BindJSON(&labels); err != nil || len(labels) == 0 {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	created, skipped, err := services.CreateLegacyPoolLabels(labels)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "创建失败", "error": err.Error()})
		return
	}
	if len(skipped) > 0 {
		ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "统计编号重复：" + strings.Join(skipped, ",") + "，已跳过", "data": gin.H{"created": created, "skipped": len(skipped)}})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "创建成功"})
}

// GetLabel 返回统计池标签，并补充关联统计器的网关和测点信息。
func GetLabel(ctx *gin.Context) {
	poolID, _ := strconv.Atoi(ctx.Query("poolID"))
	labels, err := services.GetLabel(poolID)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "查询失败", "error": err.Error()})
		return
	}
	var ids []string
	for _, v := range labels {
		ids = append(ids, v.StatsCode)
	}
	accs, _ := services.GetAccByCode(ids)
	accMap := map[string]model.Accumulate{}
	for _, v := range accs {
		accMap[v.ID] = v
	}
	var data []model.GetLabel
	for _, v := range labels {
		acc := accMap[v.StatsCode]
		data = append(data, model.GetLabel{ID: v.ID, XH: v.XH, PoolID: v.PoolID, LabelID: v.LabelID, LabelName: v.LabelName, StatsCode: v.StatsCode, StatsName: v.StatsName, GatewayID: acc.GatewayID, DataId: acc.Point})
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": data, "total": len(labels)})
}

// GetLabelByApiKey 通过 API Key 返回统计池标签选项，并兼容不同参数名和类型筛选。
func GetLabelByApiKey(ctx *gin.Context) {
	apiKey := strings.TrimSpace(ctx.Query("apiKey"))
	if apiKey == "" {
		apiKey = strings.TrimSpace(ctx.Query("Apikey"))
	}
	if apiKey == "" {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "apiKey不能为空"})
		return
	}
	statsTypes := collectStatsTypes(ctx)
	labels, err := services.FindLabelsByApiKey(apiKey, statsTypes)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "查询失败", "error": err.Error()})
		return
	}
	data := buildDataPoolLabelOptions(labels)
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": data, "total": len(data)})
}

// collectStatsTypes 兼容数组、逗号分隔文本和单值三种统计类型参数形式。
func collectStatsTypes(ctx *gin.Context) []string {
	statsTypes := ctx.QueryArray("statsType")
	if statsTypesText := ctx.Query("statsTypes"); statsTypesText != "" {
		statsTypes = append(statsTypes, strings.Split(statsTypesText, ",")...)
	}
	if statsTypeText := ctx.Query("statsType"); statsTypeText != "" && len(statsTypes) == 0 {
		statsTypes = append(statsTypes, statsTypeText)
	}
	return statsTypes
}

// buildDataPoolLabelOptions 补充统计类型前缀及前端使用的“ID:名称”组合字段。
func buildDataPoolLabelOptions(labels []model.StatsDataPoolInfo) []model.StatsDataPoolLabelOption {
	var data []model.StatsDataPoolLabelOption
	for _, label := range labels {
		statsCode := strings.TrimSpace(label.StatsCode)
		statsType := ""
		if statsCode != "" {
			statsType = strings.ToUpper(statsCode[:1])
		}
		data = append(data, model.StatsDataPoolLabelOption{
			ID:        label.ID,
			XH:        label.XH,
			PoolID:    label.PoolID,
			LabelID:   label.LabelID,
			LabelName: label.LabelName,
			StatsCode: label.StatsCode,
			StatsName: label.StatsName,
			StatsType: statsType,
			IDAndName: label.StatsCode + ":" + label.StatsName,
		})
	}
	return data
}

// UpdateLabel 更新单个统计池标签。
func UpdateLabel(ctx *gin.Context) {
	var label model.StatsDataPoolInfo
	if err := ctx.BindJSON(&label); err != nil || label.ID == 0 {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	if err := services.UpdateLabel(label); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "修改失败", "error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "修改成功"})
}

// DelLabel 按主键列表删除统计池标签。
func DelLabel(ctx *gin.Context) {
	var ids []uint
	if err := ctx.BindJSON(&ids); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	_ = services.DelLabelsById(ids)
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "删除成功"})
}

// GetStatsPoolAccData 根据 function 参数分发统计池实时或历史数据查询。
// 历史模式可按请求追加尚未结束的当前周期统计值。
func GetStatsPoolAccData(ctx *gin.Context) {
	function := ctx.Query("function")
	apiKey := ctx.Query("apiKey")
	switch function {
	case "realData":
		var labelIDs []string
		_ = ctx.BindJSON(&labelIDs)
		labels, err := services.FindLabelListByLabelIds(apiKey, labelIDs)
		if err != nil || len(labels) == 0 {
			ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "无数据", "error": err})
			return
		}
		ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": buildRealDataRes(labels), "total": len(labels)})
	case "history":
		var req model.HistoryReq
		if err := ctx.BindJSON(&req); err != nil {
			ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
			return
		}
		labels, err := services.FindLabelListByApiKeyAndLabelID(apiKey, req.LabelIDs)
		if err != nil || len(labels) == 0 {
			ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "无数据", "error": err})
			return
		}
		var codes []string
		for _, v := range labels {
			codes = append(codes, v.StatsCode)
		}
		accVals := services.FindAccRealDataByLabelID(codes)
		accMap := map[string]model.AccStatsValue{}
		for _, v := range accVals {
			accMap[v.ID] = v
		}
		var data []model.RealDataRes
		for _, label := range labels {
			data = append(data, services.FindStatsHistoryByTime(req, label, accMap[label.StatsCode])...)
			if req.RealData {
				data = append(data, buildOneRealDataRes(label, accMap[label.StatsCode]))
			}
		}
		ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": data, "total": len(data)})
	default:
		ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "请输入function参数，history或者realData"})
	}
}

// OutputLabel 将选中的统计池标签导出为临时 Excel 文件，并在响应结束后清理文件。
func OutputLabel(ctx *gin.Context) {
	var ids []int
	if err := ctx.BindJSON(&ids); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	labels, err := services.FindLabelsByIds(ids)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "获取数据失败", "error": err.Error()})
		return
	}
	f := excelize.NewFile()
	headers := []string{"序号", "标签名称", "标签id", "统计器编码", "统计器名称"}
	for i, h := range headers {
		f.SetCellValue("Sheet1", fmt.Sprintf("%c1", 'A'+i), h)
	}
	for i, v := range labels {
		row := i + 2
		f.SetCellValue("Sheet1", fmt.Sprintf("A%d", row), v.XH)
		f.SetCellValue("Sheet1", fmt.Sprintf("B%d", row), v.LabelName)
		f.SetCellValue("Sheet1", fmt.Sprintf("C%d", row), v.LabelID)
		f.SetCellValue("Sheet1", fmt.Sprintf("D%d", row), v.StatsCode)
		f.SetCellValue("Sheet1", fmt.Sprintf("E%d", row), v.StatsName)
	}
	filePath := fmt.Sprintf("统计器标签%s.xlsx", time.Now().Format("2006-01-02-15-04-05"))
	if err = f.SaveAs(filePath); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "导出失败", "error": err.Error()})
		return
	}
	ctx.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"; filename*=UTF-8''%s", url.QueryEscape(filePath), url.QueryEscape(filePath)))
	ctx.Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	ctx.File(filePath)
	defer os.Remove(filePath)
}

// BatchGetLabel 按 API Key 批量返回统计池及其完整标签列表。
func BatchGetLabel(ctx *gin.Context) {
	var apiKeys []string
	if err := ctx.BindJSON(&apiKeys); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	pools, err := services.BatchGetLabel(apiKeys)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "查询失败", "error": err.Error()})
		return
	}
	var data []model.BatchPoolData
	for _, v := range pools {
		info, _ := services.GetLabel(int(v.ID))
		data = append(data, model.BatchPoolData{ID: v.ID, PoolName: v.PoolName, ApiKey: v.ApiKey, Admin: v.Admin, Info: info})
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": data})
}

// buildRealDataRes 批量加载标签对应的统计状态并构造实时响应。
func buildRealDataRes(labels []model.StatsDataPoolInfo) []model.RealDataRes {
	var codes []string
	for _, v := range labels {
		codes = append(codes, v.StatsCode)
	}
	accVals := services.FindAccRealDataByLabelID(codes)
	accMap := map[string]model.AccStatsValue{}
	for _, v := range accVals {
		accMap[v.ID] = v
	}
	var res []model.RealDataRes
	for _, label := range labels {
		res = append(res, buildOneRealDataRes(label, accMap[label.StatsCode]))
	}
	return res
}

// buildOneRealDataRes 将一个标签和统计器当前状态组合成对外实时数据。
func buildOneRealDataRes(label model.StatsDataPoolInfo, acc model.AccStatsValue) model.RealDataRes {
	value, tt := utils.RealDataCalculate(acc)
	return model.RealDataRes{LabelID: label.LabelID, StatsCode: label.StatsCode, Value: utils.RoundStatsValue(value), StatsName: label.StatsName, LabelName: label.LabelName, Time: tt, StatsType: services.StatsTypeName(acc.Type)}
}
