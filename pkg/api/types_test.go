package api

import (
	"reflect"
	"testing"
)

func TestResourceThresholdsRound(t *testing.T) {
	limit := ResourceThresholds{
		"cpu": 100 / 3,
		"mem": 100 / 3,
	}
	expected := ResourceThresholds{
		"cpu": 33,
		"mem": 33,
	}
	if !reflect.DeepEqual(limit, expected) {
		t.Errorf("Expected %v, got %v", expected, limit)
	}
}
