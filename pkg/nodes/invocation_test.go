package nodes

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func TestPrepareExecutionPlanCanonicalHash(t *testing.T) {
	descriptor := invocationDescriptor(RiskWrite)
	first, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"cwd":"/srv/app","argv":["git","status"]}`)),
		descriptor,
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"argv":["git","status"],"cwd":"/srv/app"}`)),
		descriptor,
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.PlanHash != second.PlanHash {
		t.Fatalf("equivalent plans have different hashes: %q != %q", first.PlanHash, second.PlanHash)
	}
	second.TimeoutSeconds++
	if err := second.Validate(); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("tampered plan Validate() error = %v", err)
	}
}

func TestExecutionPlanRecomputedMutationDoesNotMatchRetainedHash(t *testing.T) {
	plan, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"argv":["git","status"]}`)),
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	retainedHash := plan.PlanHash
	plan.TimeoutSeconds++
	plan.PlanHash, err = plan.computeHash()
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("recomputed plan is not self-consistent: %v", err)
	}
	if err := plan.ValidateAgainstHash(retainedHash); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("modified plan retained-hash error = %v", err)
	}
}

func TestPrepareExecutionPlanValidatesCommandInput(t *testing.T) {
	request := invocationRequest(json.RawMessage(`{"cwd":"/srv/app"}`))
	if _, err := PrepareExecutionPlan(
		request,
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	); !errors.Is(
		err,
		ErrInvalidInvocation,
	) {
		t.Fatalf("invalid input error = %v", err)
	}
	request.Input = json.RawMessage(`{"argv":["git"],"argv":["status"]}`)
	if _, err := PrepareExecutionPlan(
		request,
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	); !errors.Is(
		err,
		ErrInvalidInvocation,
	) {
		t.Fatalf("duplicate input member error = %v", err)
	}
}

func TestPrepareExecutionPlanBoundsTrustedLifetime(t *testing.T) {
	request := invocationRequest(json.RawMessage(`{"argv":["git","status"]}`))
	if _, err := PrepareExecutionPlan(
		request,
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		time.Unix(1, 0),
		MaxExecutionPlanTTL+time.Second,
	); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("overlong plan lifetime error = %v", err)
	}
}

func TestExecutionPlanRejectsExtremeTimestampLifetime(t *testing.T) {
	plan, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"argv":["git","status"]}`)),
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan.ExpiresAt = math.MaxInt64
	plan.PlanHash, err = plan.computeHash()
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("extreme lifetime Validate() error = %v", err)
	}
}

func TestLocalCommandPolicyRejectsPlanTooFarInFuture(t *testing.T) {
	descriptor := invocationDescriptor(RiskWrite)
	preparedAt := time.Unix(1000, 0)
	plan, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"argv":["git","status"]}`)),
		descriptor,
		"local",
		"policy-1",
		preparedAt,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := LocalCommandPolicy{
		Revision:          "policy-1",
		AllowedCommands:   []string{descriptor.Name},
		MaximumRisk:       RiskWrite,
		MaxTimeoutSeconds: 60,
		MaxOutputBytes:    1024,
	}
	if err := policy.Authorize(plan, descriptor, preparedAt.Add(-MaxExecutionPlanSkew)); err != nil {
		t.Fatalf("Authorize() rejected bounded clock skew: %v", err)
	}
	if err := policy.Authorize(
		plan,
		descriptor,
		preparedAt.Add(-MaxExecutionPlanSkew-time.Second),
	); !errors.Is(
		err,
		ErrCommandDenied,
	) {
		t.Fatalf("future plan Authorize() error = %v", err)
	}
}

func TestRegistrationApprovedCommandIntersectsCatalogAndApproval(t *testing.T) {
	descriptor := invocationDescriptor(RiskWrite)
	registration := Registration{
		Snapshot: Snapshot{
			ID:      ID("node_test"),
			State:   StateConnected,
			Catalog: CapabilityCatalog{Commands: []CommandDescriptor{descriptor}},
		},
		AllowedCommands: []string{descriptor.Name},
	}
	if got, err := registration.ApprovedCommand(descriptor.Name); err != nil || got.Name != descriptor.Name {
		t.Fatalf("ApprovedCommand() = %#v, %v", got, err)
	}
	registration.AllowedCommands = nil
	if _, err := registration.ApprovedCommand(descriptor.Name); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("unapproved command error = %v", err)
	}
	registration.AllowedCommands = []string{descriptor.Name}
	registration.Snapshot.State = StateDisconnected
	if _, err := registration.ApprovedCommand(descriptor.Name); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("disconnected command error = %v", err)
	}
}

func TestLocalCommandPolicyCannotBeBroadenedByPlan(t *testing.T) {
	descriptor := invocationDescriptor(RiskWrite)
	plan, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"argv":["git","status"]}`)),
		descriptor,
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := LocalCommandPolicy{
		Revision:          "policy-1",
		AllowedCommands:   []string{descriptor.Name},
		MaximumRisk:       RiskWrite,
		MaxTimeoutSeconds: 60,
		MaxOutputBytes:    1024,
	}
	now := time.Unix(plan.PreparedAt, 0)
	if err := policy.Authorize(plan, descriptor, now); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*LocalCommandPolicy)
	}{
		{name: "command denied", mutate: func(value *LocalCommandPolicy) { value.AllowedCommands = nil }},
		{name: "risk denied", mutate: func(value *LocalCommandPolicy) { value.MaximumRisk = RiskRead }},
		{name: "timeout denied", mutate: func(value *LocalCommandPolicy) { value.MaxTimeoutSeconds = 10 }},
		{name: "output denied", mutate: func(value *LocalCommandPolicy) { value.MaxOutputBytes = 512 }},
		{name: "revision denied", mutate: func(value *LocalCommandPolicy) { value.Revision = "policy-2" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := policy
			test.mutate(&candidate)
			if err := candidate.Authorize(plan, descriptor, now); !errors.Is(err, ErrCommandDenied) {
				t.Fatalf("Authorize() error = %v", err)
			}
		})
	}
	if err := policy.Authorize(plan, descriptor, time.Unix(plan.ExpiresAt, 0)); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("expired plan Authorize() error = %v", err)
	}
}

func invocationDescriptor(risk Risk) CommandDescriptor {
	return CommandDescriptor{
		Name: "system.exec.v1",
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"argv":{"type":"array","items":{"type":"string"},"minItems":1},
				"cwd":{"type":"string"}
			},
			"required":["argv"],
			"additionalProperties":false
		}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         risk,
	}
}

func invocationRequest(input json.RawMessage) InvocationRequest {
	return InvocationRequest{
		InvocationID:     "inv_test",
		IdempotencyKey:   "idem_test",
		NodeID:           ID("node_test"),
		Command:          "system.exec.v1",
		Input:            input,
		AgentID:          "main",
		SessionID:        "session_test",
		ActorID:          "user_test",
		TimeoutSeconds:   30,
		OutputLimitBytes: 1024,
	}
}
