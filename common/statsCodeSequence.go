package common

import (
	"NewStatis6.0/model"
	"errors"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The sequence table is required; business indexes are checked independently.
func initStatsCodeSequence(db *gorm.DB) error {
	if err := db.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(&model.StatsCodeSequence{}); err != nil {
		return err
	}
	return seedStatsCodeSequence(db)
}

// Conflict handling preserves existing counters when instances start together.
// The creation transaction rechecks current codes before allocating numbers.
func seedStatsCodeSequence(db *gorm.DB) error {
	var codes, labelCodes []string
	if err := db.Model(&model.Accumulate{}).Pluck("id", &codes).Error; err != nil {
		return err
	}
	if err := db.Model(&model.StatsDataPoolInfo{}).Pluck("statsCode", &labelCodes).Error; err != nil {
		return err
	}
	sequences, err := buildStatsCodeSequences(append(codes, labelCodes...))
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(&sequences).Error
}

func buildStatsCodeSequences(codes []string) ([]model.StatsCodeSequence, error) {
	sequences := []model.StatsCodeSequence{{Type: 0, NextValue: 1}, {Type: 1, NextValue: 1}, {Type: 2, NextValue: 1}, {Type: 3, NextValue: 1}}
	for _, code := range codes {
		if len(code) < 2 {
			continue
		}
		typ := strings.IndexByte("BHDM", code[0])
		if typ < 0 {
			continue
		}
		digits := true
		for _, c := range code[1:] {
			if c < '0' || c > '9' {
				digits = false
				break
			}
		}
		if !digits {
			continue
		}
		n, err := strconv.ParseUint(code[1:], 10, 64)
		if err != nil || n == ^uint64(0) {
			return nil, errors.New("统计编号超出范围，无法初始化编号表")
		}
		if n >= sequences[typ].NextValue {
			sequences[typ].NextValue = n + 1
		}
	}
	return sequences, nil
}
