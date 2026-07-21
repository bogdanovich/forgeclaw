package nodes

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
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

func TestInvocationRequestRejectsNonCanonicalCatalogHash(t *testing.T) {
	t.Parallel()

	request := invocationRequest(json.RawMessage(`{"argv":["git","status"]}`))
	request.CatalogHash = strings.ToUpper(request.CatalogHash)
	if err := request.Validate(); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("uppercase catalog hash error = %v", err)
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

func TestPrepareExecutionPlanValidatesNumericCommandInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		schema string
	}{
		{
			name:   "integer",
			input:  `{"count":1}`,
			schema: `{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false}`,
		},
		{
			name:   "number",
			input:  `{"ratio":0.1}`,
			schema: `{"type":"object","properties":{"ratio":{"type":"number"}},"required":["ratio"],"additionalProperties":false}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			descriptor := invocationDescriptor(RiskWrite)
			descriptor.InputSchema = json.RawMessage(test.schema)
			request := invocationRequest(json.RawMessage(test.input))

			if _, err := PrepareExecutionPlan(
				request,
				descriptor,
				"local",
				"policy-1",
				time.Unix(1_700_000_000, 0),
				time.Minute,
			); err != nil {
				t.Fatalf("PrepareExecutionPlan() error = %v", err)
			}
		})
	}
}

func TestExecutionPlanRejectsNumericPrecisionBypass(t *testing.T) {
	t.Parallel()

	restrictive := invocationDescriptor(RiskWrite)
	restrictive.InputSchema = json.RawMessage(`{
		"type":"object",
		"properties":{"count":{"type":"integer","maximum":9007199254740992}},
		"required":["count"],
		"additionalProperties":false
	}`)
	request := invocationRequest(json.RawMessage(`{"count":9007199254740993}`))
	restrictiveHash, err := invocationCatalog(restrictive).Hash()
	if err != nil {
		t.Fatal(err)
	}
	request.CatalogHash = restrictiveHash
	if _, planErr := PrepareExecutionPlan(
		request,
		restrictive,
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	); !errors.Is(planErr, ErrInvalidInvocation) {
		t.Fatalf("large integer PrepareExecutionPlan() error = %v", planErr)
	}

	permissive := restrictive
	permissive.InputSchema = json.RawMessage(`{
		"type":"object",
		"properties":{"count":{"type":"integer"}},
		"required":["count"],
		"additionalProperties":false
	}`)
	plan, err := PrepareExecutionPlan(
		request,
		permissive,
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
		AllowedCommands:   []string{plan.Command},
		MaximumRisk:       RiskWrite,
		MaxTimeoutSeconds: 60,
		MaxOutputBytes:    1024,
	}
	if err := policy.Authorize(
		plan,
		invocationCatalog(restrictive),
		plan.NodeID,
		plan.Executor,
		time.Unix(plan.PreparedAt, 0),
	); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("large integer Authorize() error = %v", err)
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

func TestPrepareExecutionPlanBoundsCanonicalInput(t *testing.T) {
	t.Parallel()

	input := json.RawMessage(`{"argv":["` + strings.Repeat("<", 100_000) + `"]}`)
	if len(input) >= MaxInvocationInputBytes {
		t.Fatalf("test input unexpectedly exceeds raw limit: %d", len(input))
	}
	if _, err := PrepareExecutionPlan(
		invocationRequest(input),
		invocationDescriptor(RiskWrite),
		"local",
		"policy-1",
		time.Unix(1, 0),
		time.Minute,
	); !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("expanded canonical input error = %v", err)
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
	if err := policy.Authorize(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		plan.Executor,
		preparedAt.Add(-MaxExecutionPlanSkew),
	); err != nil {
		t.Fatalf("Authorize() rejected bounded clock skew: %v", err)
	}
	if err := policy.Authorize(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		plan.Executor,
		preparedAt.Add(-MaxExecutionPlanSkew-time.Second),
	); !errors.Is(
		err,
		ErrCommandDenied,
	) {
		t.Fatalf("future plan Authorize() error = %v", err)
	}
}

func TestLocalCommandPolicyAuthorizesExpiredReplayWithoutBroadeningAuthority(t *testing.T) {
	descriptor := invocationDescriptor(RiskWrite)
	plan, err := PrepareExecutionPlan(
		invocationRequest(json.RawMessage(`{"argv":["git","status"]}`)),
		descriptor,
		"local",
		"policy-1",
		time.Unix(100, 0),
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := LocalCommandPolicy{
		Revision:          "policy-1",
		AllowedCommands:   []string{descriptor.Name},
		MaximumRisk:       RiskWrite,
		MaxTimeoutSeconds: 60,
		MaxOutputBytes:    4096,
	}
	if err := policy.AuthorizeReplay(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		plan.Executor,
	); err != nil {
		t.Fatalf("AuthorizeReplay() rejected expired recorded plan: %v", err)
	}
	policy.AllowedCommands = nil
	if err := policy.AuthorizeReplay(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		plan.Executor,
	); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("revoked replay authority error = %v", err)
	}
}

func TestRegistrationApprovedCommandIntersectsCatalogAndApproval(t *testing.T) {
	descriptor := invocationDescriptor(RiskWrite)
	catalog := invocationCatalog(descriptor)
	catalogHash, hashErr := catalog.Hash()
	if hashErr != nil {
		t.Fatal(hashErr)
	}
	registration := Registration{
		Snapshot: Snapshot{
			ID:          ID("node_test"),
			State:       StateConnected,
			CatalogHash: catalogHash,
			Catalog:     catalog,
		},
		AllowedCommands:     []string{descriptor.Name},
		ApprovedCatalogHash: catalogHash,
		ApprovedAt:          1,
	}
	if got, err := registration.ApprovedCommand(descriptor.Name); err != nil || got.Name != descriptor.Name {
		t.Fatalf("ApprovedCommand() = %#v, %v", got, err)
	}
	registration.ApprovedAt = 0
	if _, err := registration.ApprovedCommand(descriptor.Name); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("missing approval timestamp error = %v", err)
	}
	registration.ApprovedAt = 1
	registration.AllowedCommands = nil
	if _, err := registration.ApprovedCommand(descriptor.Name); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("unapproved command error = %v", err)
	}
	registration.AllowedCommands = []string{descriptor.Name}
	changed := descriptor
	changed.OutputSchema = json.RawMessage(`{"type":"object","properties":{"changed":{"type":"boolean"}}}`)
	registration.Snapshot.Catalog = invocationCatalog(changed)
	changedHash, hashErr := registration.Snapshot.Catalog.Hash()
	if hashErr != nil {
		t.Fatal(hashErr)
	}
	registration.Snapshot.CatalogHash = changedHash
	if _, err := registration.ApprovedCommand(descriptor.Name); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("changed catalog error = %v", err)
	}
	registration.Snapshot.Catalog = catalog
	registration.Snapshot.CatalogHash = catalogHash
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
	if err := policy.Authorize(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		plan.Executor,
		now,
	); err != nil {
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
			if err := candidate.Authorize(
				plan,
				invocationCatalog(descriptor),
				plan.NodeID,
				plan.Executor,
				now,
			); !errors.Is(err, ErrCommandDenied) {
				t.Fatalf("Authorize() error = %v", err)
			}
		})
	}
	if err := policy.Authorize(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		plan.Executor,
		time.Unix(plan.ExpiresAt, 0),
	); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("expired plan Authorize() error = %v", err)
	}
	if err := policy.Authorize(
		plan,
		invocationCatalog(descriptor),
		ID("node_other"),
		plan.Executor,
		now,
	); !errors.Is(
		err,
		ErrCommandDenied,
	) {
		t.Fatalf("wrong-node Authorize() error = %v", err)
	}
	if err := policy.Authorize(
		plan,
		invocationCatalog(descriptor),
		plan.NodeID,
		"docker",
		now,
	); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("wrong-executor Authorize() error = %v", err)
	}
	changedDescriptor := descriptor
	changedDescriptor.InputSchema = json.RawMessage(`{"type":"object"}`)
	if err := policy.Authorize(
		plan,
		invocationCatalog(changedDescriptor),
		plan.NodeID,
		plan.Executor,
		now,
	); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("wrong-catalog Authorize() error = %v", err)
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

func invocationCatalog(descriptor CommandDescriptor) CapabilityCatalog {
	return CapabilityCatalog{Commands: []CommandDescriptor{descriptor}}
}

func invocationRequest(input json.RawMessage) InvocationRequest {
	catalogHash, err := invocationCatalog(invocationDescriptor(RiskWrite)).Hash()
	if err != nil {
		panic(err)
	}
	return InvocationRequest{
		InvocationID:     "inv_test",
		IdempotencyKey:   "idem_test",
		NodeID:           ID("node_test"),
		CatalogHash:      catalogHash,
		Command:          "system.exec.v1",
		Input:            input,
		AgentID:          "main",
		SessionID:        "session_test",
		ActorID:          "user_test",
		TimeoutSeconds:   30,
		OutputLimitBytes: 1024,
	}
}
