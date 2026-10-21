// Package routerServer 集中注册统计器、统计池和对外查询接口。
package routerServer

import (
	"NewStatis6.0/controller"
	"NewStatis6.0/middleware"

	"github.com/gin-gonic/gin"
)

// ConnectRoute 为 Gin 引擎挂载中间件和全部业务路由。
func ConnectRoute(router *gin.Engine) *gin.Engine {
	router.Use(middleware.Cors())

	// 统计器配置、统计历史、实时值和导入导出接口。
	statsAccumulate := router.Group("/go/stats/accumulate")
	{
		statsAccumulate.POST("/create", controller.StatsAccumulateCreate)
		statsAccumulate.POST("/getStats", controller.GetStatsAccumulate)
		statsAccumulate.POST("/getStatsByType", controller.GetStatsByType)
		statsAccumulate.DELETE("/delete", controller.DeleteAccumulate)
		statsAccumulate.POST("/getStatsByIds", controller.GetStatsByIds)
		statsAccumulate.POST("/getHistory", controller.AccGetHistory)
		statsAccumulate.POST("/getHistoryByList", controller.AccGetHistoryByList)
		statsAccumulate.POST("/getEnergyBoardSummary", controller.AccGetEnergyBoardSummary)
		statsAccumulate.GET("/getIdAndName", controller.AccGetIdAndName)
		statsAccumulate.GET("/getDevice", controller.AccGetDevice)
		statsAccumulate.POST("/accUpload", controller.AccUpload)
		statsAccumulate.POST("/output", controller.AccOutput)
	}

	// 统计池及其标签管理接口。
	statsDataPool := router.Group("/go/stats")
	{
		statsDataPool.POST("/createPool", controller.CreatePool)
		statsDataPool.PUT("/updatePool", controller.UpdatePool)
		statsDataPool.POST("/getPool", controller.GetPool)
		statsDataPool.DELETE("/delPool", controller.DelPool)
		statsDataPool.POST("/createLabel", controller.CreateLabel)
		statsDataPool.POST("/createLabelsFromPoints", controller.CreateLabelsFromPoints)
		statsDataPool.GET("/getLabel", controller.GetLabel)
		statsDataPool.GET("/getLabelByApiKey", controller.GetLabelByApiKey)
		statsDataPool.PUT("/updateLabel", controller.UpdateLabel)
		statsDataPool.DELETE("/delLabel", controller.DelLabel)
		statsDataPool.POST("/batchGetLabel", controller.BatchGetLabel)
		statsDataPool.POST("/outputLabel", controller.OutputLabel)
	}

	// 提供给大屏等外部调用方的统计池数据接口。
	statsPoolData := router.Group("/stats/pool")
	{
		statsPoolData.POST("/getAccData", controller.GetStatsPoolAccData)
	}
	return router
}
