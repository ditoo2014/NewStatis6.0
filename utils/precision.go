package utils

import "math"

const statsDecimalScale = 1000

// RoundStatsValue 将统计结果四舍五入到最多三位小数，并消除负零。
func RoundStatsValue(value float64) float64 {
	rounded := math.Round(value*statsDecimalScale) / statsDecimalScale
	if rounded == 0 {
		return 0
	}
	return rounded
}
