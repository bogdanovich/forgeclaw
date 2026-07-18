package seahorse

import (
	"encoding/json"
	"testing"
)

func TestConfigUnmarshalsAbsoluteContextBudgets(t *testing.T) {
	var config Config
	if err := json.Unmarshal([]byte(`{
		"historyMaxTokens": 12000,
		"summaryMaxTokens": 4000,
		"recentTailTurns": 3
	}`), &config); err != nil {
		t.Fatal(err)
	}
	if config.HistoryMaxTokens != 12000 ||
		config.SummaryMaxTokens != 4000 ||
		config.RecentTailTurns != 3 {
		t.Fatalf("unexpected context budget config: %#v", config)
	}
}
