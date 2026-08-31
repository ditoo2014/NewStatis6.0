package utils

import (
	"encoding/json"
	"math"
	"testing"
)

func TestRoundStatsValue(t *testing.T) {
	tests := []struct {
		name  string
		value float64
		want  float64
	}{
		{name: "integer", value: 1, want: 1},
		{name: "one decimal", value: 1.2, want: 1.2},
		{name: "three decimals", value: 1.234, want: 1.234},
		{name: "round down", value: 1.2344, want: 1.234},
		{name: "round up", value: 1.2345, want: 1.235},
		{name: "small value to zero", value: 0.0004, want: 0},
		{name: "small value up", value: 0.0005, want: 0.001},
		{name: "negative value", value: -1.2345, want: -1.235},
		{name: "precision tail", value: 0.0000010000003, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RoundStatsValue(tt.value); got != tt.want {
				t.Fatalf("RoundStatsValue(%v) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestRoundStatsValueNormalizesNegativeZero(t *testing.T) {
	got := RoundStatsValue(math.Copysign(0.0001, -1))
	if got != 0 || math.Signbit(got) {
		t.Fatalf("expected positive zero, got %v (signbit=%v)", got, math.Signbit(got))
	}
}

func TestApplyFormulaPreservesSmallRawValue(t *testing.T) {
	got, err := ApplyFormula("x*10000000", 0.0000001)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("expected 1, got %v", got)
	}
}

func TestRoundedStatsValueMarshalsAsJSONNumberWithoutFixedZeros(t *testing.T) {
	payload := struct {
		Value float64 `json:"value"`
	}{Value: RoundStatsValue(1.2)}

	got, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"value":1.2}` {
		t.Fatalf("expected numeric JSON without fixed zeros, got %s", got)
	}
}
