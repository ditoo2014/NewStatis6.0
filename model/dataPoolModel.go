package model

// StatsDataPool 是对外提供统计数据的一组池配置。
type StatsDataPool struct {
	ID       uint   `gorm:"type:integer;column:id;primaryKey;autoIncrement" json:"id"`
	PoolName string `gorm:"type:varchar(255);column:poolName" json:"poolName"`
	ApiKey   string `gorm:"type:varchar(255);column:apiKey" json:"apiKey"`
	Admin    string `gorm:"type:varchar(100);column:admin" json:"admin"`
}

// StatsDataPoolInfo 关联统计池标签与具体统计器。
type StatsDataPoolInfo struct {
	ID        uint   `gorm:"type:integer;column:id;primaryKey;autoIncrement" json:"id"`
	XH        uint   `gorm:"type:integer;column:xh" json:"xh"`
	PoolID    uint   `gorm:"type:integer;column:poolID" json:"poolID"`
	LabelID   string `gorm:"type:varchar(255);column:labelID" json:"labelID"`
	LabelName string `gorm:"type:varchar(255);column:labelName" json:"labelName"`
	StatsCode string `gorm:"type:varchar(255);column:statsCode" json:"statsCode"`
	StatsName string `gorm:"type:varchar(255);column:statsName" json:"statsName"`
}

// StatsDataPoolLabelOption 是标签管理页面展示的统计器选项。
type StatsDataPoolLabelOption struct {
	ID        uint   `json:"id"`
	XH        uint   `json:"xh"`
	PoolID    uint   `json:"poolID"`
	LabelID   string `json:"labelID"`
	LabelName string `json:"labelName"`
	StatsCode string `json:"statsCode"`
	StatsName string `json:"statsName"`
	StatsType string `json:"statsType"`
	IDAndName string `json:"idAndName"`
}

// GetLabel 是兼容既有标签查询接口的响应结构。
type GetLabel struct {
	ID        uint   `json:"id"`
	XH        uint   `json:"xh"`
	PoolID    uint   `json:"poolID"`
	LabelID   string `json:"labelID"`
	LabelName string `json:"labelName"`
	StatsCode string `json:"statsCode"`
	StatsName string `json:"statsName"`
	GatewayID string `json:"gatewayID"`
	DataId    string `json:"dataId"`
}

// PageInfo 是统计池列表的分页和管理员筛选条件。
type PageInfo struct {
	Search string `json:"search"`
	Page   int    `json:"page"`
	Size   int    `json:"size"`
	Admin  string `json:"admin"`
}

// HistoryReq 是外部统计池历史数据查询条件。
type HistoryReq struct {
	LabelIDs  []string `json:"labelIDs"`
	StartTime int64    `json:"startTime"`
	EndTime   int64    `json:"endTime"`
	RealData  bool     `json:"realData"`
}

// RealDataRes 是统计池对外返回的统一实时或历史数据结构。
type RealDataRes struct {
	LabelID   string  `json:"labelID"`
	StatsCode string  `json:"statsCode"`
	Value     float64 `json:"value"`
	StatsName string  `json:"statsName"`
	LabelName string  `json:"labelName"`
	Time      string  `json:"time"`
	StatsType string  `json:"statsType"`
}

// BatchPoolData 将统计池及其全部标签组合为批量查询响应。
type BatchPoolData struct {
	ID       uint                `json:"id"`
	PoolName string              `json:"poolName"`
	ApiKey   string              `json:"apiKey"`
	Admin    string              `json:"admin"`
	Info     []StatsDataPoolInfo `json:"info"`
}

// TableName 固定统计池配置表名。
func (StatsDataPool) TableName() string {
	return "stats_data_pool"
}

// TableName 固定统计池标签明细表名。
func (StatsDataPoolInfo) TableName() string {
	return "stats_data_pool_info"
}
