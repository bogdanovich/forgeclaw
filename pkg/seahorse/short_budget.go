package seahorse

import "fmt"

func (c Config) validateBudgets() error {
	if c.HistoryMaxTokens < 0 {
		return fmt.Errorf("historyMaxTokens must be non-negative")
	}
	if c.SummaryMaxTokens < 0 {
		return fmt.Errorf("summaryMaxTokens must be non-negative")
	}
	if c.RecentTailTurns < 0 {
		return fmt.Errorf("recentTailTurns must be non-negative")
	}
	return nil
}

func (c Config) absoluteBudgetsEnabled() bool {
	return c.HistoryMaxTokens > 0 || c.SummaryMaxTokens > 0 || c.RecentTailTurns > 0
}

func (c Config) historyBudget(totalBudget int) int {
	return boundedBudget(c.HistoryMaxTokens, totalBudget)
}

func (c Config) summaryBudget(totalBudget int) int {
	return boundedBudget(c.SummaryMaxTokens, totalBudget)
}

func boundedBudget(configured, total int) int {
	if configured > 0 && (total <= 0 || configured < total) {
		return configured
	}
	return total
}

func minPositive(values ...int) int {
	result := 0
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if result == 0 || value < result {
			result = value
		}
	}
	return result
}
