package companion

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
)

func TestParseSystemdLogsEnforcesRecordTotalAndBinaryBounds(t *testing.T) {
	longMessage := strings.Repeat("x", nodes.MaxServiceLogRecordBytes+100)
	raw := []byte(fmt.Sprintf(
		"{\"__REALTIME_TIMESTAMP\":\"1700000001000000\",\"PRIORITY\":\"7\",\"MESSAGE\":%q}\n",
		longMessage,
	))
	logs, err := parseSystemdLogs("vpn", raw, 1, nodes.MaxServiceLogBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !logs.Truncated || len(logs.Records) != 1 ||
		len(logs.Records[0].Message) > nodes.MaxServiceLogRecordBytes {
		t.Fatalf("record truncation = %#v", logs)
	}

	many := strings.Repeat(
		`{"__REALTIME_TIMESTAMP":"1700000001000000","MESSAGE":"bounded message"}`+"\n",
		20,
	)
	logs, err = parseSystemdLogs("vpn", []byte(many), 20, 256)
	if err != nil {
		t.Fatal(err)
	}
	if !logs.Truncated || len(logs.Records) == 0 || !serviceLogsFit(logs, 256) {
		t.Fatalf("total truncation = %#v", logs)
	}

	for _, malformed := range []string{
		`{"__REALTIME_TIMESTAMP":"1700000001000000","MESSAGE":[1,2]}`,
		`{"__REALTIME_TIMESTAMP":"1700000001000000","MESSAGE":"a","MESSAGE":"b"}`,
		`{"__REALTIME_TIMESTAMP":"999999","MESSAGE":"too early"}`,
	} {
		_, parseErr := parseSystemdLogs("vpn", []byte(malformed), 1, 1024)
		var managerErr *ServiceManagerError
		if !errors.As(parseErr, &managerErr) || managerErr.Code != "log_record_invalid" {
			t.Fatalf("malformed record error = %v", parseErr)
		}
	}
}

func TestParseSystemdLogsReplacesInvalidUTF8AndControls(t *testing.T) {
	raw := append(
		[]byte(`{"__REALTIME_TIMESTAMP":"1700000001000000","MESSAGE":"before`),
		0xff,
	)
	raw = append(raw, []byte(`after\u0002"}`)...)
	logs, err := parseSystemdLogs("vpn", raw, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.Records) != 1 || logs.Records[0].Message != "before�after�" {
		t.Fatalf("sanitized invalid UTF-8 record = %#v", logs.Records)
	}
}

func TestParseSystemdStatusRequiresFixedProperties(t *testing.T) {
	properties, err := parseSystemdStatus([]byte(
		"LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=\n",
	))
	if err != nil || properties["UnitFileState"] != "" {
		t.Fatalf("status properties = %#v, error %v", properties, err)
	}
	for _, malformed := range []string{
		"LoadState=loaded\nActiveState=active\nSubState=running\n",
		"LoadState=loaded\nLoadState=masked\nActiveState=active\nSubState=running\nUnitFileState=enabled\n",
		"LoadState=loaded\nActiveState=active\nSubState=running\nUnitFileState=enabled\nFragmentPath=/secret\n",
	} {
		if _, parseErr := parseSystemdStatus([]byte(malformed)); parseErr == nil {
			t.Fatalf("accepted malformed status %q", malformed)
		}
	}
}

func TestSystemdStateNormalizationUsesFixedVocabulary(t *testing.T) {
	if normalizeSystemdLoadState("merged") != "loaded" ||
		normalizeSystemdLoadState("unexpected") != "unknown" ||
		normalizeSystemdActiveState("maintenance") != "unknown" ||
		normalizeSystemdSubstate("stop-sigterm") != "stop" ||
		normalizeSystemdEnabledState("enabled-runtime") != "enabled" ||
		normalizeSystemdEnabledState("bad") != "unknown" {
		t.Fatal("systemd state normalization escaped the fixed vocabulary")
	}
}

func TestBoundedServiceLogRequestClampsOnlyConfiguredNumbers(t *testing.T) {
	limits := nodes.ServiceLogLimits{EntriesMax: 50, BytesMax: 4096, AgeSecondsMax: 60}
	entries, since, err := boundedServiceLogRequest(ServiceLogRequest{
		Entries:      100,
		SinceSeconds: 120,
	}, limits)
	if err != nil || entries != 50 || since != 60 {
		t.Fatalf("bounded request = %d, %d, error %v", entries, since, err)
	}
	if _, _, err = boundedServiceLogRequest(ServiceLogRequest{Entries: -1}, limits); err == nil {
		t.Fatal("negative log request was accepted")
	}
}
