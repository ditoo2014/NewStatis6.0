package model

// CreateLabelsFromPointsReq keeps code allocation and label creation on the server.
type CreateLabelsFromPointsReq struct {
	PoolID    uint             `json:"poolID"`
	GatewayID string           `json:"gatewayID"`
	Points    []PoolPointInput `json:"points"`
}

type PoolPointInput struct {
	DataID string `json:"dataID"`
	Name   string `json:"name"`
}

// Counts refer to labels (four labels per complete point).
type CreatePoolLabelsResult struct {
	Created       int      `json:"created"`
	Skipped       int      `json:"skipped"`
	SkippedPoints []string `json:"skippedPoints"`
}

// StatsCodeSequence is created and seeded automatically during MySQL startup.
type StatsCodeSequence struct {
	Type      int    `gorm:"column:type;primaryKey;autoIncrement:false"`
	NextValue uint64 `gorm:"type:bigint unsigned;column:next_value;not null"`
}

func (StatsCodeSequence) TableName() string { return "stats_code_sequence" }
