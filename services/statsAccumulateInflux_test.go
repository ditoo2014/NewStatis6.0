package services

import (
	"NewStatis6.0/common"
	"NewStatis6.0/config"
	"NewStatis6.0/model"
	"NewStatis6.0/utils"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

// fakeInfluxClient 记录查询次数并可注入错误，用于隔离真实 InfluxDB。
type fakeInfluxClient struct {
	mu        sync.Mutex
	queryErr  error
	queryCall int
}

// Ping 实现 InfluxDB 客户端接口。
func (f *fakeInfluxClient) Ping(time.Duration) (time.Duration, string, error) {
	return 0, "", nil
}

// Write 实现 InfluxDB 客户端接口，测试中无需实际持久化。
func (f *fakeInfluxClient) Write(client.BatchPoints) error {
	return nil
}

// Query 记录调用次数，并返回注入错误或空的成功响应。
func (f *fakeInfluxClient) Query(client.Query) (*client.Response, error) {
	f.mu.Lock()
	f.queryCall++
	err := f.queryErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &client.Response{Results: []client.Result{{}}}, nil
}

// QueryAsChunk 实现 InfluxDB 客户端接口。
func (f *fakeInfluxClient) QueryAsChunk(client.Query) (*client.ChunkedResponse, error) {
	return nil, nil
}

// Close 实现 InfluxDB 客户端接口。
func (f *fakeInfluxClient) Close() error {
	return nil
}

// TestFindBoundaryPointsInRangeUsesConfiguredBatches 验证 6000 个测点按 300 个拆批产生 40 次首末查询。
func TestFindBoundaryPointsInRangeUsesConfiguredBatches(t *testing.T) {
	oldClient := common.Influx
	oldBatchSize := config.Conf.Stats.SourceIDBatchSize
	fake := &fakeInfluxClient{}
	common.Influx = fake
	config.Conf.Stats.SourceIDBatchSize = 300
	t.Cleanup(func() {
		common.Influx = oldClient
		config.Conf.Stats.SourceIDBatchSize = oldBatchSize
	})

	accList := make([]model.Accumulate, 0, 6000)
	for i := 0; i < 6000; i++ {
		accList = append(accList, model.Accumulate{ID: fmt.Sprintf("H%d", i), GatewayID: "G1", Point: fmt.Sprintf("P%d", i)})
	}
	first, last, err := FindBoundaryPointsInRange(accList, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 || len(last) != 0 {
		t.Fatalf("expected empty point maps, got first=%d last=%d", len(first), len(last))
	}
	if fake.queryCall != 40 {
		t.Fatalf("expected 40 batched queries, got %d", fake.queryCall)
	}
}

// TestQueryFirstPointReturnsInfluxError 验证查询失败不会被误判为成功但无数据。
func TestQueryFirstPointReturnsInfluxError(t *testing.T) {
	oldClient := common.Influx
	wantErr := errors.New("influx unavailable")
	common.Influx = &fakeInfluxClient{queryErr: wantErr}
	t.Cleanup(func() { common.Influx = oldClient })

	_, hasData, err := queryFirstPoint("SELECT value FROM test")
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected query error, got %v", err)
	}
	if hasData {
		t.Fatal("query error must not be treated as valid data")
	}
}

// TestWriteHistoryPointRoundsStoredValue 验证历史统计值在加入 InfluxDB 批次前保留最多三位小数。
func TestWriteHistoryPointRoundsStoredValue(t *testing.T) {
	oldMeasurement := config.Conf.Influx.HistoryMeasurement
	oldLocation := config.Location
	config.Conf.Influx.HistoryMeasurement = "StatsAcc"
	config.Location = time.Local
	t.Cleanup(func() {
		config.Conf.Influx.HistoryMeasurement = oldMeasurement
		config.Location = oldLocation
	})

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{Database: "test"})
	if err != nil {
		t.Fatal(err)
	}
	err = WriteHistoryPoint(model.Accumulate{ID: "H1", Type: 0}, utils.Period{Start: 100, End: 200}, 1.23456, bp)
	if err != nil {
		t.Fatal(err)
	}
	if len(bp.Points()) != 1 {
		t.Fatalf("expected one history point, got %d", len(bp.Points()))
	}
	fields, err := bp.Points()[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if got := fields["data"]; got != float64(1.235) {
		t.Fatalf("expected stored value 1.235, got %v", got)
	}
}
