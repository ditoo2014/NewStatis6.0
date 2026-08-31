package algorithm

import (
	"NewStatis6.0/config"
	"NewStatis6.0/model"
	"NewStatis6.0/services"
	"NewStatis6.0/utils"
	"testing"
	"time"
)

// setupAlgorithmTest 固定测试时区和负增量策略，并在用例结束后恢复全局配置。
func setupAlgorithmTest(t *testing.T) {
	t.Helper()
	oldLocation := config.Location
	oldPolicy := config.Conf.Stats.NegativeDeltaPolicy
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	config.Location = location
	config.Conf.Stats.NegativeDeltaPolicy = "zero"
	t.Cleanup(func() {
		config.Location = oldLocation
		config.Conf.Stats.NegativeDeltaPolicy = oldPolicy
	})
}

// TestApplyCurrentPointsStartsAfterGap 验证断档恢复后以当前周期首点作为新起点。
func TestApplyCurrentPointsStartsAfterGap(t *testing.T) {
	record := model.AccStatsValue{ID: "H1", Type: 1, Gap: true}
	first := services.PointValue{Time: 100, Value: 20}
	last := services.PointValue{Time: 200, Value: 35}

	got := applyCurrentPoints(record, first, last)
	if got.StartTime != first.Time || got.StartValue != first.Value {
		t.Fatalf("unexpected start point: %+v", got)
	}
	if got.EndTime != last.Time || got.EndValue != last.Value || !got.HasData || got.Gap {
		t.Fatalf("unexpected current state: %+v", got)
	}
}

// TestApplyCurrentPointsKeepsContinuousStart 验证连续周期不会覆盖上一周期留下的基线。
func TestApplyCurrentPointsKeepsContinuousStart(t *testing.T) {
	record := model.AccStatsValue{
		ID: "H1", Type: 1, StartTime: 50, StartValue: 10,
		EndTime: 50, EndValue: 10, HasData: true,
	}
	got := applyCurrentPoints(record,
		services.PointValue{Time: 100, Value: 20},
		services.PointValue{Time: 200, Value: 35},
	)
	if got.StartTime != 50 || got.StartValue != 10 {
		t.Fatalf("continuous start point changed: %+v", got)
	}
	if got.EndTime != 200 || got.EndValue != 35 {
		t.Fatalf("unexpected end point: %+v", got)
	}
}

// TestClosePeriodRecordWithoutDataMarksGap 验证空周期值为 0 且下一周期保持断档状态。
func TestClosePeriodRecordWithoutDataMarksGap(t *testing.T) {
	setupAlgorithmTest(t)
	period := utils.BuildPeriod(0, time.Date(2026, 8, 5, 14, 10, 0, 0, config.Location).Unix())
	record := model.AccStatsValue{
		ID: "B1", Type: 0, PeriodStartTime: period.Start, PeriodEndTime: period.End,
		StartTime: period.Start + 60, StartValue: 100, EndTime: period.Start + 120,
		EndValue: 110, HasData: true,
	}

	next, value := closePeriodRecord(model.Accumulate{ID: "B1", Type: 0}, record, services.PointValue{}, services.PointValue{}, false)
	if value != 0 || next.PeriodStartTime != period.End || next.PeriodEndTime != utils.NextPeriod(period).End {
		t.Fatalf("unexpected no-data close result: value=%v state=%+v", value, next)
	}
	if next.HasData || !next.Gap {
		t.Fatalf("no-data period did not mark a gap: %+v", next)
	}
}

// TestClosePeriodRecordUsesPreviousEndForContinuousPeriod 验证连续周期使用前序末值计算增量。
func TestClosePeriodRecordUsesPreviousEndForContinuousPeriod(t *testing.T) {
	setupAlgorithmTest(t)
	period := utils.BuildPeriod(0, time.Date(2026, 8, 5, 14, 10, 0, 0, config.Location).Unix())
	record := model.AccStatsValue{
		ID: "B1", Type: 0, PeriodStartTime: period.Start, PeriodEndTime: period.End,
		StartTime: period.Start - 60, StartValue: 100, EndTime: period.Start - 60,
		EndValue: 100, HasData: true,
	}
	first := services.PointValue{Time: period.Start + 60, Value: 105}
	last := services.PointValue{Time: period.End - 60, Value: 130}

	next, value := closePeriodRecord(model.Accumulate{ID: "B1", Type: 0}, record, first, last, true)
	if value != 30 {
		t.Fatalf("expected continuous delta 30, got %v", value)
	}
	if next.StartTime != last.Time || next.StartValue != last.Value || next.Gap || !next.HasData {
		t.Fatalf("unexpected next-period baseline: %+v", next)
	}
}

// TestClosePeriodRecordRestartsAfterGap 验证恢复周期只计算本周期首末有效点之差。
func TestClosePeriodRecordRestartsAfterGap(t *testing.T) {
	setupAlgorithmTest(t)
	period := utils.BuildPeriod(1, time.Date(2026, 8, 5, 14, 0, 0, 0, config.Location).Unix())
	record := model.AccStatsValue{
		ID: "H1", Type: 1, PeriodStartTime: period.Start, PeriodEndTime: period.End,
		StartTime: period.Start - 60, StartValue: 100, EndTime: period.Start - 60,
		EndValue: 100, Gap: true,
	}
	first := services.PointValue{Time: period.Start + 60, Value: 140}
	last := services.PointValue{Time: period.End - 60, Value: 155}

	next, value := closePeriodRecord(model.Accumulate{ID: "H1", Type: 1}, record, first, last, true)
	if value != 15 {
		t.Fatalf("expected recovery delta 15, got %v", value)
	}
	if next.StartValue != 155 || next.Gap || !next.HasData {
		t.Fatalf("unexpected recovery state: %+v", next)
	}
}
