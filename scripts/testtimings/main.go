package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

const timingLimit = 20

type testEvent struct {
	Time    time.Time
	Action  string
	Package string
	Test    string
	Elapsed float64
}

type timing struct {
	name    string
	elapsed float64
}

type timingSummary struct {
	started  time.Time
	finished time.Time
	packages []timing
	tests    []timing
}

func main() {
	summary, err := readTimingSummary(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "summarize Go test timings: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(renderTimingSummary(summary))
}

func readTimingSummary(reader io.Reader) (timingSummary, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	packages := map[string]float64{}
	var tests []timing
	var started time.Time
	var finished time.Time

	for scanner.Scan() {
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return timingSummary{}, fmt.Errorf("decode event: %w", err)
		}
		if !event.Time.IsZero() {
			if started.IsZero() || event.Time.Before(started) {
				started = event.Time
			}
			if finished.IsZero() || event.Time.After(finished) {
				finished = event.Time
			}
		}
		if event.Action != "pass" && event.Action != "fail" {
			continue
		}
		if event.Test == "" {
			if event.Package != "" {
				packages[event.Package] = event.Elapsed
			}
			continue
		}
		tests = append(tests, timing{
			name:    event.Package + "/" + event.Test,
			elapsed: event.Elapsed,
		})
	}
	if err := scanner.Err(); err != nil {
		return timingSummary{}, fmt.Errorf("read events: %w", err)
	}

	packageTimings := make([]timing, 0, len(packages))
	for name, elapsed := range packages {
		packageTimings = append(packageTimings, timing{name: name, elapsed: elapsed})
	}
	sortTimings(packageTimings)
	sortTimings(tests)
	return timingSummary{
		started:  started,
		finished: finished,
		packages: packageTimings,
		tests:    tests,
	}, nil
}

func sortTimings(timings []timing) {
	sort.Slice(timings, func(i, j int) bool {
		if timings[i].elapsed == timings[j].elapsed {
			return timings[i].name < timings[j].name
		}
		return timings[i].elapsed > timings[j].elapsed
	})
}

func renderTimingSummary(summary timingSummary) string {
	var output strings.Builder
	output.WriteString("## Go test timing\n\n")
	if !summary.started.IsZero() && !summary.finished.IsZero() {
		fmt.Fprintf(&output, "Observed event span: %.3fs\n\n", summary.finished.Sub(summary.started).Seconds())
	}
	writeTimingTable(&output, "Slowest packages", summary.packages)
	output.WriteByte('\n')
	writeTimingTable(&output, "Slowest tests", summary.tests)
	return output.String()
}

func writeTimingTable(output *strings.Builder, title string, timings []timing) {
	fmt.Fprintf(output, "### %s\n\n| Name | Elapsed |\n| --- | ---: |\n", title)
	limit := min(len(timings), timingLimit)
	for _, item := range timings[:limit] {
		name := strings.ReplaceAll(item.name, "|", "\\|")
		fmt.Fprintf(output, "| `%s` | %.3fs |\n", name, item.elapsed)
	}
	if limit == 0 {
		output.WriteString("| _(no completed entries)_ | 0.000s |\n")
	}
}
