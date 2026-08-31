// Package controller 负责 Gin 请求校验、业务流程编排和兼容既有接口响应。
package controller

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"NewStatis6.0/algorithm"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/model"
	"NewStatis6.0/services"
	"NewStatis6.0/utils"

	"github.com/gin-gonic/gin"
	"github.com/xuri/excelize/v2"
)

// StatsAccumulateCreate 批量创建或更新统计器配置。
// 更新前暂停对应调度，完成后刷新内存配置并立即唤醒统计任务。
func StatsAccumulateCreate(ctx *gin.Context) {
	var list []model.Accumulate
	if err := ctx.BindJSON(&list); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
		return
	}
	var creates, updates []model.Accumulate
	for _, v := range list {
		exist, match := services.FindAccumulateById(v)
		if !exist {
			creates = append(creates, v)
		} else if !match {
			updates = append(updates, v)
		}
	}
	if len(creates) > 0 {
		if err := services.CreateAccumulate(creates); err != nil {
			algorithm.GetAllAccumulate()
			ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "创建失败", "error": err.Error()})
			return
		}
	}
	if len(updates) > 0 {
		updateIDs := make([]string, 0, len(updates))
		for _, item := range updates {
			updateIDs = append(updateIDs, item.ID)
		}
		algorithm.SuspendAccumulates(updateIDs)
		if err := services.UpdateAccumulate(updates); err != nil {
			algorithm.GetAllAccumulate()
			algorithm.NotifyAccumulates()
			ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "修改失败", "error": err.Error()})
			return
		}
	}
	algorithm.GetAllAccumulate()
	algorithm.NotifyAccumulates()
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "创建成功"})
}

// GetStatsAccumulate 分页返回全部类型的统计器配置。
func GetStatsAccumulate(ctx *gin.Context) {
	var page model.AccPageInfo
	if err := ctx.BindJSON(&page); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	normalizePage(&page)
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "list": services.GetAccumulateList(page), "total": services.GetAccumulateTotal(page)})
}

// GetStatsByType 分页返回 URL 参数指定类型的统计器配置。
func GetStatsByType(ctx *gin.Context) {
	var page model.AccPageInfo
	typeNum, _ := strconv.Atoi(ctx.Query("type"))
	if err := ctx.BindJSON(&page); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	normalizePage(&page)
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "list": services.GetAccumulateList2(page, typeNum), "total": services.GetAccumulateTotal2(page, typeNum)})
}

// DeleteAccumulate 删除统计器配置和计算状态，并按配置同步或异步清理统计历史。
// 原始采集数据始终保留；删除期间会先暂停对应统计器，避免并发写回状态。
func DeleteAccumulate(ctx *gin.Context) {
	var delList model.IDList
	if err := ctx.BindJSON(&delList); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	algorithm.SuspendAccumulates(delList.List)
	taskCount, asyncHistoryDelete, err := services.DelAccumulateWithHistoryTasks(delList)
	if err != nil {
		algorithm.GetAllAccumulate()
		algorithm.NotifyAccumulates()
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "删除失败", "error": err.Error()})
		return
	}
	algorithm.GetAllAccumulate()
	algorithm.NotifyAccumulates()
	if asyncHistoryDelete {
		services.NotifyAccHistoryDeleteWorker()
		ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "配置删除成功，历史数据后台清理中", "taskCount": taskCount})
		return
	}
	if config.Conf.Delete.Enabled() {
		services.DeleteHistoryByIDs(delList.List)
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "删除成功"})
}

// AccGetHistory 查询统计报表页面的单组统计器历史及可选实时值。
func AccGetHistory(ctx *gin.Context) {
	var page model.AccFormPage
	if err := ctx.BindJSON(&page); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
		return
	}
	normalizeAccFormPage(&page)
	accList := services.SearchAccList(page)
	defaultQuery := isDefaultAccHistoryQuery(page)
	forms, total, err := queryAccFormPage(accList, page, page.RealTime, defaultQuery)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "查询失败", "error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "\u67e5\u8be2\u6210\u529f", "formList": forms, "total": total})
}

// AccGetHistoryByList 按多个“ID:名称”组合查询统计历史，并在服务端完成分页。
func AccGetHistoryByList(ctx *gin.Context) {
	var page model.AccFormPageByIDAndName
	if err := ctx.BindJSON(&page); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
		return
	}
	if page.Page == 0 {
		page.Page = 1
	}
	if page.Size == 0 {
		page.Size = 10
	}
	accList := services.AccListByIDAndName(page)
	accPage := model.AccFormPage{StartTime: page.StartTime, EndTime: page.EndTime, Type: page.Type, Page: page.Page, Size: page.Size}
	forms, total, err := queryAccFormPage(accList, accPage, page.RealTime, false)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "查询失败", "error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "formList": forms, "total": total})
}

// queryAccFormPage 先批量统计各统计器的历史条数，再只查询目标页覆盖的数据段。
// 开启实时值时，每个统计器的当前周期结果排在其历史记录之前并计入总数。
func queryAccFormPage(accList []model.Accumulate, page model.AccFormPage, realTime, defaultQuery bool) ([]model.AccForm, int, error) {
	historyCountMap, err := services.CountAccFormInfoMap(page, accList, defaultQuery)
	if err != nil {
		return nil, 0, err
	}
	realValueMap := map[string]model.AccStatsValue{}
	if realTime {
		ids := make([]string, 0, len(accList))
		for _, acc := range accList {
			ids = append(ids, acc.ID)
		}
		var err error
		realValueMap, err = services.LoadAccStatsValueMap(ids)
		if err != nil {
			return nil, 0, err
		}
	}
	start := (page.Page - 1) * page.Size
	end := start + page.Size
	total := 0
	forms := make([]model.AccForm, 0, page.Size)
	for _, acc := range accList {
		accStart := total
		historyCount := historyCountMap[acc.ID]
		realCount := 0
		record, hasReal := realValueMap[acc.ID]
		if realTime && hasReal && record.ID != "" {
			realCount = 1
		}
		accTotal := realCount + historyCount
		total += accTotal
		if accTotal == 0 || accStart >= end || total <= start {
			continue
		}
		offset := start - accStart
		if offset < 0 {
			offset = 0
		}
		limit := end - accStart - offset
		if limit > accTotal-offset {
			limit = accTotal - offset
		}
		if limit <= 0 {
			continue
		}
		if realCount == 1 && offset == 0 {
			if realForm, ok := buildRealAccFormByRecord(acc, record); ok {
				forms = append(forms, realForm)
				limit--
			}
			offset = 0
		} else if realCount == 1 {
			offset--
		}
		if limit > 0 {
			historyForms, err := services.SelectAccFormInfoPageE(page, acc, defaultQuery, offset, limit)
			if err != nil {
				return nil, 0, err
			}
			forms = append(forms, historyForms...)
		}
	}
	return forms, total, nil
}

// AccGetEnergyBoardSummary 汇总能源看板的今日、本周、本月、本年值和本月日明细。
func AccGetEnergyBoardSummary(ctx *gin.Context) {
	var req model.EnergyBoardSummaryReq
	if err := ctx.BindJSON(&req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
		return
	}

	dayForms := queryEnergyAccForms(req.DailyIDAndNameList, 2, req.DayRange)
	weekForms := queryEnergyAccForms(req.DailyIDAndNameList, 2, req.WeekRange)
	monthForms := queryEnergyAccForms(req.MonthlyIDAndNameList, 3, req.MonthRange)
	yearForms := queryEnergyAccForms(req.MonthlyIDAndNameList, 3, req.YearRange)
	monthDailyForms := queryEnergyAccForms(req.DailyIDAndNameList, 2, req.MonthRange)

	data := model.EnergyBoardSummaryRes{
		Today:          sumAccForms(dayForms),
		Week:           sumAccForms(weekForms),
		Month:          sumAccForms(monthForms),
		Year:           sumAccForms(yearForms),
		MonthDailyList: monthDailyForms,
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": data})
}

// queryEnergyAccForms 查询一组统计器在指定时间范围内的历史，并按需合并当前周期值。
func queryEnergyAccForms(idAndNameList []string, typ int, rng model.AccTimeRange) []model.AccForm {
	if len(idAndNameList) == 0 {
		return []model.AccForm{}
	}
	page := model.AccFormPageByIDAndName{IDAndNameList: idAndNameList, Type: typ}
	accList := services.AccListByIDAndName(page)
	accPage := model.AccFormPage{StartTime: rng.StartTime, EndTime: rng.EndTime, Type: typ}
	var forms []model.AccForm
	for _, acc := range accList {
		items := services.SelectAccFormInfo(accPage, acc)
		if rng.RealTime {
			if realForm, ok := buildRealAccFormInRange(acc, rng); ok {
				items = append([]model.AccForm{realForm}, items...)
			}
		}
		forms = append(forms, items...)
	}
	return dedupeAccForms(forms)
}

// buildRealAccFormInRange 仅在当前统计周期与请求范围相交时构造实时报表项。
func buildRealAccFormInRange(acc model.Accumulate, rng model.AccTimeRange) (model.AccForm, bool) {
	record := services.GetAccStatsValue(acc.ID)
	if record.ID == "" {
		return model.AccForm{}, false
	}
	periodEnd := record.PeriodEndTime
	if periodEnd == 0 {
		periodEnd = utils.BuildPeriod(acc.Type, record.PeriodStartTime).End
	}
	if !timeRangeOverlaps(rng, record.PeriodStartTime, periodEnd) {
		return model.AccForm{}, false
	}
	raw, tt := utils.RealDataCalculate(record)
	value, err := utils.ApplyFormula(acc.Count, raw)
	if err != nil {
		value = 0
	}
	return model.AccForm{ID: acc.ID, StatsName: acc.StatsName, Type: acc.Type, Time: tt, DeviceType: acc.DeviceType, Value: utils.RoundStatsValue(value), Notes: acc.Notes}, true
}

// timeRangeOverlaps 判断统计周期与可选起止时间是否相交。
func timeRangeOverlaps(rng model.AccTimeRange, startTime, endTime int64) bool {
	if startTime == 0 {
		return false
	}
	if endTime == 0 {
		endTime = startTime
	}
	if rng.StartTime > 0 && endTime <= rng.StartTime {
		return false
	}
	if rng.EndTime > 0 && startTime > rng.EndTime {
		return false
	}
	return true
}

// dedupeAccForms 按统计器 ID 和显示周期去重，避免实时值与已落库历史重复。
func dedupeAccForms(forms []model.AccForm) []model.AccForm {
	seen := map[string]bool{}
	result := make([]model.AccForm, 0, len(forms))
	for _, form := range forms {
		key := form.ID + "|" + form.Time
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, form)
	}
	return result
}

// sumAccForms 汇总报表项的统计值。
func sumAccForms(forms []model.AccForm) float64 {
	var total float64
	for _, form := range forms {
		total += form.Value
	}
	return utils.RoundStatsValue(total)
}

// AccGetIdAndName 返回指定类型统计器的前端下拉选项。
func AccGetIdAndName(ctx *gin.Context) {
	ty, _ := strconv.Atoi(ctx.Query("type"))
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": services.AccGetIdAndName(ty)})
}

// AccGetReportIdAndName 返回指定项目报表白名单中的统计器“ID:名称”列表。
// tianma项目专用接口
func AccGetReportIdAndName(ctx *gin.Context) {
	ty, err := strconv.Atoi(ctx.Query("type"))
	if err != nil || ty < 0 || ty > 3 {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "type参数错误"})
		return
	}
	project := ctx.Query("project")
	if project != "tianma" {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "project参数错误"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": services.AccGetReportIdAndName(ty, project)})
}

// AccGetDevice 返回统计器配置中可用的设备类型。
func AccGetDevice(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "data": services.AccGetDevice()})
}

// AccUpload 从 Excel 导入统计器配置，并通过采集云校验网关和测点名称。
// 导入完成后刷新调度器；历史补算由后台队列继续执行，不阻塞上传响应。
func AccUpload(ctx *gin.Context) {
	file, err := ctx.FormFile("file")
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
		return
	}
	tempFile, err := os.CreateTemp("", "upload-*.xlsx")
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "保存到临时文件失败", "error": err.Error()})
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	if err = ctx.SaveUploadedFile(file, tempFile.Name()); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "保存上传文件失败", "error": err.Error()})
		return
	}
	f, err := excelize.OpenFile(tempFile.Name())
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "打开失败", "error": err.Error()})
		return
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "Excel没有工作表"})
		return
	}
	rows, err := f.GetRows(sheets[0])
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "读取Excel失败", "error": err.Error()})
		return
	}

	token, err := utils.LoginAndGetToken()
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "采集云登录失败", "error": err.Error()})
		return
	}
	gateways, err := utils.GetGatewayData(token)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "获取网关失败", "error": err.Error()})
		return
	}
	gatewayMap := make(map[string]string, len(gateways))
	for _, gateway := range gateways {
		gatewayMap[gateway.Name] = gateway.GatewayID
	}
	pointCache := make(map[string]map[string]string)

	var list []model.Accumulate
	for i, row := range rows[1:] {
		rowNo := i + 2
		acc, err := parseAccUploadRow(row, rowNo, token, gatewayMap, pointCache)
		if err != nil {
			ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": err.Error()})
			return
		}
		list = append(list, acc)
	}
	if len(list) == 0 {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "Excel没有可导入的数据"})
		return
	}
	if err = saveAccumulateList(list); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "上传失败", "error": err.Error()})
		return
	}
	algorithm.GetAllAccumulate()
	algorithm.NotifyAccumulates()
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "上传成功"})
}

// parseAccUploadRow 将一行 Excel 数据转换为统计器配置，并校验必填字段。
// pointCache 按网关缓存测点名称映射，避免同一网关重复调用采集云接口。
func parseAccUploadRow(row []string, rowNo int, token string, gatewayMap map[string]string, pointCache map[string]map[string]string) (model.Accumulate, error) {
	value := func(index int) string {
		if len(row) <= index {
			return ""
		}
		return row[index]
	}
	acc := model.Accumulate{ID: value(0), StatsName: value(1), StartTime: value(5), DeviceType: value(6), Count: value(7), Notes: value(8)}
	if acc.ID == "" {
		return acc, fmt.Errorf("第%d行数据出错,编号不存在", rowNo)
	}
	if acc.StatsName == "" {
		return acc, fmt.Errorf("%s统计器名称不存在", acc.ID)
	}
	gatewayName := value(2)
	if gatewayName == "" {
		return acc, fmt.Errorf("%s网关不存在", acc.ID)
	}
	gatewayID := gatewayMap[gatewayName]
	if gatewayID == "" {
		return acc, fmt.Errorf("%s网关不存在:%s", acc.ID, gatewayName)
	}
	acc.GatewayID = gatewayID
	pointName := value(3)
	if pointName == "" {
		return acc, fmt.Errorf("%s数据点不存在", acc.ID)
	}
	pointMap, ok := pointCache[gatewayID]
	if !ok {
		points, err := utils.GetPointData(token, gatewayID)
		if err != nil {
			return acc, fmt.Errorf("%s数据点获取失败:%v", acc.ID, err)
		}
		pointMap = make(map[string]string, len(points))
		for _, point := range points {
			pointMap[point.Name] = point.DataID
		}
		pointCache[gatewayID] = pointMap
	}
	pointID := pointMap[pointName]
	if pointID == "" {
		return acc, fmt.Errorf("%s数据点不存在:%s", acc.ID, pointName)
	}
	acc.Point = pointID
	typ, ok := accTypeFromName(value(4))
	if !ok {
		return acc, fmt.Errorf("%s统计类型不存在或不正确", acc.ID)
	}
	acc.Type = typ
	if acc.StartTime == "" {
		return acc, fmt.Errorf("%s开始时间不存在", acc.ID)
	}
	if acc.DeviceType == "" {
		return acc, fmt.Errorf("%s设备类型不存在", acc.ID)
	}
	return acc, nil
}

// saveAccumulateList 将导入数据拆分为新增项和变更项，并安全暂停需要重算的统计器。
func saveAccumulateList(list []model.Accumulate) error {
	var creates, updates []model.Accumulate
	for _, v := range list {
		exist, match := services.FindAccumulateById(v)
		if !exist {
			creates = append(creates, v)
		} else if !match {
			updates = append(updates, v)
		}
	}
	if len(creates) > 0 {
		if err := services.CreateAccumulate(creates); err != nil {
			return err
		}
	}
	if len(updates) > 0 {
		updateIDs := make([]string, 0, len(updates))
		for _, item := range updates {
			updateIDs = append(updateIDs, item.ID)
		}
		algorithm.SuspendAccumulates(updateIDs)
		if err := services.UpdateAccumulate(updates); err != nil {
			algorithm.GetAllAccumulate()
			algorithm.NotifyAccumulates()
			return err
		}
	}
	return nil
}

// accTypeFromName 将导入模板中的兼容中文名称转换为统计类型编号。
func accTypeFromName(name string) (int, bool) {
	switch name {
	case "半小时增量统计", "半小时统计":
		return 0, true
	case "小时增量统计", "小时统计":
		return 1, true
	case "日增量统计", "日统计":
		return 2, true
	case "月增量统计", "月统计":
		return 3, true
	default:
		return 0, false
	}
}

// accTypeName 返回导入导出模板使用的统计类型名称。
func accTypeName(typ int) string {
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

// accOutputName 返回不同统计类型对应的导出文件名前缀。
func accOutputName(typ int) string {
	switch typ {
	case 0:
		return "累计值半小时统计"
	case 1:
		return "累计值小时统计"
	case 2:
		return "累计值日统计"
	case 3:
		return "累计值月统计"
	default:
		return "累计值统计"
	}
}

// AccOutput 导出选中或指定类型的统计器配置，并将网关、测点 ID 还原为名称。
func AccOutput(ctx *gin.Context) {
	var req model.AccOutput
	if err := ctx.BindJSON(&req); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误", "error": err.Error()})
		return
	}
	var accList []model.Accumulate
	if len(req.IDList) == 0 {
		accList = services.GetAccList(req.Type)
	} else {
		accList = services.GetAccListByIdList(req.IDList)
	}
	token, err := utils.LoginAndGetToken()
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "采集云登录失败", "error": err.Error()})
		return
	}
	gateways, err := utils.GetGatewayData(token)
	if err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 500, "msg": "获取网关失败", "error": err.Error()})
		return
	}
	gatewayNameMap := make(map[string]string, len(gateways))
	for _, gateway := range gateways {
		gatewayNameMap[gateway.GatewayID] = gateway.Name
	}
	pointNameCache := make(map[string]map[string]string)
	f := excelize.NewFile()
	headers := []string{"编号", "名称", "网关", "数据点", "统计类型", "开始时间", "设备类型", "公式", "备注"}
	for i, h := range headers {
		f.SetCellValue("Sheet1", fmt.Sprintf("%c1", 'A'+i), h)
	}
	for i, a := range accList {
		row := i + 2
		gatewayName := gatewayNameMap[a.GatewayID]
		if gatewayName == "" {
			gatewayName = a.GatewayID
		}
		pointName := a.Point
		pointMap, ok := pointNameCache[a.GatewayID]
		if !ok {
			points, err := utils.GetPointData(token, a.GatewayID)
			if err != nil {
				logger.Error("output get point failed", err)
			}
			pointMap = make(map[string]string, len(points))
			for _, point := range points {
				pointMap[point.DataID] = point.Name
			}
			pointNameCache[a.GatewayID] = pointMap
		}
		if name := pointMap[a.Point]; name != "" {
			pointName = name
		}
		f.SetCellValue("Sheet1", fmt.Sprintf("A%d", row), a.ID)
		f.SetCellValue("Sheet1", fmt.Sprintf("B%d", row), a.StatsName)
		f.SetCellValue("Sheet1", fmt.Sprintf("C%d", row), gatewayName)
		f.SetCellValue("Sheet1", fmt.Sprintf("D%d", row), pointName)
		f.SetCellValue("Sheet1", fmt.Sprintf("E%d", row), accTypeName(a.Type))
		f.SetCellValue("Sheet1", fmt.Sprintf("F%d", row), a.StartTime)
		f.SetCellValue("Sheet1", fmt.Sprintf("G%d", row), a.DeviceType)
		f.SetCellValue("Sheet1", fmt.Sprintf("H%d", row), a.Count)
		f.SetCellValue("Sheet1", fmt.Sprintf("I%d", row), a.Notes)
	}
	filePath := fmt.Sprintf("%s%s.xlsx", accOutputName(req.Type), time.Now().Format("2006-01-02-15-04-05"))
	if err := f.SaveAs(filePath); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "导出失败", "error": err.Error()})
		return
	}
	ctx.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"; filename*=UTF-8''%s", url.QueryEscape(filePath), url.QueryEscape(filePath)))
	ctx.Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	ctx.File(filePath)
	defer os.Remove(filePath)
}

// GetStatsByIds 按请求中的 ID 集合返回统计器配置。
func GetStatsByIds(ctx *gin.Context) {
	var ids []string
	if err := ctx.BindJSON(&ids); err != nil {
		ctx.JSON(http.StatusOK, gin.H{"code": 400, "msg": "参数错误"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"code": 200, "msg": "查询成功", "list": services.GetStatsByIds(ids)})
}

// buildRealAccForm 读取统计器当前状态并转换为实时报表项。
func buildRealAccForm(acc model.Accumulate) (model.AccForm, bool) {
	record := services.GetAccStatsValue(acc.ID)
	return buildRealAccFormByRecord(acc, record)
}

// buildRealAccFormByRecord 根据已加载状态计算实时增量并应用统计公式。
func buildRealAccFormByRecord(acc model.Accumulate, record model.AccStatsValue) (model.AccForm, bool) {
	if record.ID == "" {
		return model.AccForm{}, false
	}
	raw, tt := utils.RealDataCalculate(record)
	value, err := utils.ApplyFormula(acc.Count, raw)
	if err != nil {
		value = 0
	}
	return model.AccForm{ID: acc.ID, StatsName: acc.StatsName, Type: acc.Type, Time: tt, DeviceType: acc.DeviceType, Value: utils.RoundStatsValue(value), Notes: acc.Notes}, true
}

// isDefaultAccHistoryQuery 判断请求是否未提供任何统计器或时间筛选条件。
func isDefaultAccHistoryQuery(page model.AccFormPage) bool {
	return page.IDAndName == "" && page.StartTime == 0 && page.EndTime == 0 && page.DeviceType == "" && page.Search == ""
}

// normalizePage 为统计器配置列表补充分页默认值。
func normalizePage(page *model.AccPageInfo) {
	if page.Page <= 0 {
		page.Page = 1
	}
	if page.Size <= 0 {
		page.Size = 10
	}
}

// normalizeAccFormPage 为统计报表查询补充分页默认值。
func normalizeAccFormPage(page *model.AccFormPage) {
	if page.Page <= 0 {
		page.Page = 1
	}
	if page.Size <= 0 {
		page.Size = 10
	}
}

// paginateForms 对已在内存中的报表结果执行边界安全的兼容分页。
func paginateForms(forms []model.AccForm, page, size int) []model.AccForm {
	start := (page - 1) * size
	if start >= len(forms) {
		return []model.AccForm{}
	}
	end := start + size
	if end > len(forms) {
		end = len(forms)
	}
	return forms[start:end]
}
