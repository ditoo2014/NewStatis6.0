package utils

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"NewStatis6.0/model"
)

// JsonNumberToFloat32 将 InfluxDB 返回的 JSON 数值转换为保留四位精度的 float32。
// 转换失败时保持既有接口语义并返回 0。
func JsonNumberToFloat32(jsonNumber json.Number) float32 {
	floatValue, err := jsonNumber.Float64()
	if err != nil {
		return 0
	}
	formattedValue := fmt.Sprintf("%.4f", float32(floatValue))
	finalValue, err := strconv.ParseFloat(formattedValue, 32)
	if err != nil {
		return 0
	}
	return float32(finalValue)
}

// GenerateUniqueID 生成由 12 个加密随机字节编码而成的 24 位十六进制 ID。
func GenerateUniqueID() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// GenerateRandomNumericString 生成固定 14 位的随机数字字符串，用作统计池 API Key。
func GenerateRandomNumericString() (string, error) {
	bytes := make([]byte, 7)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	num := int64(0)
	for _, b := range bytes {
		num = num*256 + int64(b)
	}
	return fmt.Sprintf("%014d", num%10000000000000), nil
}

// RealDataCalculate 根据当前周期保存的起止值计算实时增量。
// 周期没有有效数据或计数器回退时返回 0，时间仍指向当前统计周期。
func RealDataCalculate(acc model.AccStatsValue) (newVal float64, currentTime string) {
	if !acc.HasData {
		return 0, FormatPeriodTime(acc.Type, acc.PeriodStartTime)
	}
	raw := acc.EndValue - acc.StartValue
	if raw < 0 {
		raw = 0
	}
	return raw, FormatPeriodTime(acc.Type, acc.PeriodStartTime)
}
