package main

import (
	"strings"
	"testing"
)

func TestReadAndRenderTimingSummary(t *testing.T) {
	events := strings.NewReader(strings.Join([]string{
		`{"Time":"2026-08-02T12:00:00Z","Action":"start","Package":"example/fast"}`,
		`{"Time":"2026-08-02T12:00:01Z","Action":"pass","Package":"example/fast","Test":"TestFast","Elapsed":1}`,
		`{"Time":"2026-08-02T12:00:02Z","Action":"pass","Package":"example/fast","Elapsed":2}`,
		`{"Time":"2026-08-02T12:00:03Z","Action":"fail","Package":"example/slow","Test":"TestSlow","Elapsed":3}`,
		`{"Time":"2026-08-02T12:00:04Z","Action":"fail","Package":"example/slow","Elapsed":4}`,
	}, "\n"))

	summary, err := readTimingSummary(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.packages) != 2 || summary.packages[0].name != "example/slow" {
		t.Fatalf("package timings = %+v", summary.packages)
	}
	if len(summary.tests) != 2 || summary.tests[0].name != "example/slow/TestSlow" {
		t.Fatalf("test timings = %+v", summary.tests)
	}

	rendered := renderTimingSummary(summary)
	for _, want := range []string{
		"Observed event span: 4.000s",
		"| `example/slow` | 4.000s |",
		"| `example/slow/TestSlow` | 3.000s |",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered summary missing %q:\n%s", want, rendered)
		}
	}
}

func TestReadTimingSummaryRejectsMalformedJSON(t *testing.T) {
	if _, err := readTimingSummary(strings.NewReader("{not-json}\n")); err == nil {
		t.Fatal("expected malformed JSON error")
	}
}
