// Package main 负责装配并启动 NewStatis6.0 统计服务。
package main

import (
	"NewStatis6.0/algorithm"
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"NewStatis6.0/routerServer"
	"NewStatis6.0/services"
	"fmt"

	"github.com/gin-gonic/gin"
)

// init 按配置、日志、MySQL、InfluxDB 的依赖顺序完成启动前初始化。
func init() {
	if err := config.LoadConfig("config/application.yml"); err != nil {
		panic(err)
	}
	if err := logger.InitLog(); err != nil {
		panic(err)
	}
	if err := common.InitMysql(); err != nil {
		panic(err)
	}
	if err := common.InitInfluxDB(); err != nil {
		panic(err)
	}
}

// main 启动统计调度、历史删除任务和 Gin HTTP 服务。
func main() {
	defer common.CloseInflux()

	router := gin.Default()
	router = routerServer.ConnectRoute(router)
	if config.Conf.Stats.Enable {
		go algorithm.StartAllAccumulate()
	}
	services.StartAccHistoryDeleteWorker()
	stopCalibration := algorithm.StartCalibration()
	defer stopCalibration()
	if err := router.Run(fmt.Sprintf(":%d", config.Conf.Server.Port)); err != nil {
		logger.Error("server run failed", err)
	}
}
