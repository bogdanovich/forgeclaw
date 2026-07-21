package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/nodes"
)

func TestRuntimeRequiresLocalCommandApproval(t *testing.T) {
	policy := testRuntimePolicy(nil)
	runtime, err := NewRuntime(nodes.ID("node_test"), "test", policy, newMemoryInvocationLedger())
	if err != nil {
		t.Fatal(err)
	}
	plan := testRuntimePlan(t, runtime, "node.info.v1", json.RawMessage(`{}`))
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(err, nodes.ErrCommandDenied) {
		t.Fatalf("Invoke() error = %v", err)
	}
}

func TestRuntimeExecutesReadOnlyBuiltins(t *testing.T) {
	policy := testRuntimePolicy([]string{"node.info.v1", "system.which.v1"})
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"v-test",
		policy,
		newMemoryInvocationLedger(),
	)
	if newErr != nil {
		t.Fatal(newErr)
	}

	info, err := runtime.Invoke(
		t.Context(),
		testRuntimePlan(t, runtime, "node.info.v1", json.RawMessage(`{}`)),
	)
	if err != nil {
		t.Fatal(err)
	}
	var infoResult struct {
		NodeID  nodes.ID `json:"node_id"`
		Version string   `json:"version"`
	}
	if decodeErr := json.Unmarshal(info, &infoResult); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if infoResult.NodeID != "node_test" || infoResult.Version != "v-test" {
		t.Fatalf("node.info result = %+v", infoResult)
	}

	which, err := runtime.Invoke(t.Context(), testRuntimePlan(
		t,
		runtime,
		"system.which.v1",
		json.RawMessage(`{"name":"go"}`),
	))
	if err != nil {
		t.Fatal(err)
	}
	var whichResult struct {
		Found bool   `json:"found"`
		Path  string `json:"path"`
	}
	if decodeErr := json.Unmarshal(which, &whichResult); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !whichResult.Found || whichResult.Path == "" {
		t.Fatalf("system.which result = %+v", whichResult)
	}
}

func TestRuntimeReturnsRecordedResultAfterPlanExpires(t *testing.T) {
	clock := time.Unix(100, 0)
	ledger := newInvocationLedger(
		"",
		DefaultInvocationLedgerLimit,
		DefaultInvocationLedgerBytes,
		func() time.Time { return clock },
	)
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testRuntimePlanAt(
		t,
		runtime,
		"node.info.v1",
		json.RawMessage(`{}`),
		clock,
		time.Second,
	)
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}
	if _, markErr := ledger.MarkRunning(plan.InvocationID); markErr != nil {
		t.Fatal(markErr)
	}
	want := json.RawMessage(
		`{"node_id":"node_test","platform":"linux","architecture":"amd64","version":"test"}`,
	)
	if _, completeErr := ledger.CompleteSuccess(plan.InvocationID, want); completeErr != nil {
		t.Fatal(completeErr)
	}
	clock = clock.Add(2 * time.Second)
	got, err := runtime.Invoke(t.Context(), plan)
	if err != nil {
		t.Fatalf("expired duplicate Invoke() error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("expired duplicate result = %s", got)
	}
}

func TestRuntimeDoesNotReplayUnfinishedInvocation(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testRuntimePlan(t, runtime, "node.info.v1", json.RawMessage(`{}`))
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}
	if _, markErr := ledger.MarkRunning(plan.InvocationID); markErr != nil {
		t.Fatal(markErr)
	}
	if _, err := runtime.Invoke(t.Context(), plan); !errors.Is(
		err,
		ErrInvocationOutcomeUnknown,
	) {
		t.Fatalf("unfinished duplicate Invoke() error = %v", err)
	}
}

func TestRuntimeResumesDurableAcceptedInvocationAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invocations.json")
	ledger, newErr := NewFileInvocationLedger(path, 4, 1024*1024)
	if newErr != nil {
		t.Fatal(newErr)
	}
	t.Cleanup(ledger.Close)
	policy := testRuntimePolicy([]string{"node.info.v1"})
	beforeRestart, newErr := NewRuntime(nodes.ID("node_test"), "test", policy, ledger)
	if newErr != nil {
		t.Fatal(newErr)
	}
	plan := testRuntimePlan(t, beforeRestart, "node.info.v1", json.RawMessage(`{}`))
	if _, _, err := ledger.Accept(plan); err != nil {
		t.Fatal(err)
	}

	ledger.Close()
	reloaded, reloadErr := NewFileInvocationLedger(path, 4, 1024*1024)
	if reloadErr != nil {
		t.Fatal(reloadErr)
	}
	t.Cleanup(reloaded.Close)
	afterRestart, newErr := NewRuntime(nodes.ID("node_test"), "test", policy, reloaded)
	if newErr != nil {
		t.Fatal(newErr)
	}
	if _, err := afterRestart.Invoke(t.Context(), plan); err != nil {
		t.Fatalf("Invoke() accepted resume error = %v", err)
	}
	record, found := reloaded.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationSucceeded {
		t.Fatalf("resumed record = %#v, found %v", record, found)
	}
}

func TestRuntimeReturnsRecordedResultWithoutExecutingAgain(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.count.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	handler := &countingHandler{}
	descriptor := handler.descriptor()
	runtime.handlers[descriptor.Name] = handler
	runtime.catalog.Commands = append(runtime.catalog.Commands, descriptor)
	plan := testRuntimePlan(t, runtime, descriptor.Name, json.RawMessage(`{}`))

	first, firstErr := runtime.Invoke(t.Context(), plan)
	second, secondErr := runtime.Invoke(t.Context(), plan)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("Invoke() errors = %v, %v", firstErr, secondErr)
	}
	if string(first) != string(second) || handler.executions != 1 {
		t.Fatalf("results = %s, %s; executions = %d", first, second, handler.executions)
	}
}

func TestRuntimeCancelsActiveInvocation(t *testing.T) {
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.block.v1"}),
		newMemoryInvocationLedger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newRuntimeBlockingHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "runtime-cancel")
	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := commandRuntime.Invoke(t.Context(), plan)
		invokeDone <- invokeErr
	}()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("invocation did not start")
	}
	record, err := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != nodes.InvocationRunning || record.Cancellation == nil ||
		record.Cancellation.TerminationConfirmed {
		t.Fatalf("cancellation acknowledgement = %#v", record)
	}
	if invokeErr := <-invokeDone; !errors.Is(invokeErr, ErrInvocationCanceled) {
		t.Fatalf("Invoke() error = %v", invokeErr)
	}
	record, found, err := commandRuntime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationCanceled ||
		record.Cancellation == nil || !record.Cancellation.TerminationConfirmed {
		t.Fatalf("canceled record = %#v, found %v, error %v", record, found, err)
	}
	repeated, err := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	})
	if err != nil || repeated.State != nodes.InvocationCanceled {
		t.Fatalf("repeated Cancel() = %#v, error %v", repeated, err)
	}
	if _, replayErr := commandRuntime.Invoke(t.Context(), plan); !errors.Is(
		replayErr,
		ErrInvocationCanceled,
	) {
		t.Fatalf("canceled replay error = %v", replayErr)
	}
}

func TestRuntimeCancellationReacquiresOwnerAfterDurableTransition(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.block.v1"}),
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newRuntimeBlockingHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "cancel-owner-handoff")
	if _, _, acceptErr := ledger.Accept(plan); acceptErr != nil {
		t.Fatal(acceptErr)
	}

	barrier := &cancellationTransitionBarrier{
		invocationStore: ledger,
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	commandRuntime.ledger = barrier
	cancelDone := make(chan error, 1)
	go func() {
		_, cancelErr := commandRuntime.Cancel(nodes.InvocationCancelRequest{
			InvocationID: plan.InvocationID,
		})
		cancelDone <- cancelErr
	}()
	<-barrier.entered

	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := commandRuntime.executeAccepted(t.Context(), plan)
		invokeDone <- invokeErr
	}()
	<-handler.started
	close(barrier.release)
	if cancelErr := <-cancelDone; cancelErr != nil {
		t.Fatal(cancelErr)
	}
	if invokeErr := <-invokeDone; !errors.Is(invokeErr, ErrInvocationCanceled) {
		t.Fatalf("Invoke() error = %v", invokeErr)
	}
	record, found, err := commandRuntime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationCanceled ||
		record.Cancellation == nil || !record.Cancellation.TerminationConfirmed {
		t.Fatalf("canceled owner-handoff record = %#v, found %v, error %v", record, found, err)
	}
}

func TestRuntimeCancellationDoesNotRewriteSuccessfulResult(t *testing.T) {
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.ignore-cancel.v1"}),
		newMemoryInvocationLedger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newIgnoringCancellationHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "ignore-cancel")
	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := commandRuntime.Invoke(t.Context(), plan)
		invokeDone <- invokeErr
	}()
	<-handler.started
	if _, cancelErr := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); cancelErr != nil {
		t.Fatal(cancelErr)
	}
	close(handler.release)
	if invokeErr := <-invokeDone; invokeErr != nil {
		t.Fatal(invokeErr)
	}
	record, found, err := commandRuntime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationSucceeded ||
		record.Cancellation != nil {
		t.Fatalf("successful cancellation race = %#v, found %v, error %v", record, found, err)
	}
}

func TestRuntimeLateCancellationDoesNotRewriteHandlerFailure(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.fail.v1"}),
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := runtimeFailingHandler{}
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "late-cancel-failure")
	barrier := &failureTransitionBarrier{
		invocationStore: ledger,
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	commandRuntime.ledger = barrier

	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := commandRuntime.Invoke(t.Context(), plan)
		invokeDone <- invokeErr
	}()
	<-barrier.entered
	if _, cancelErr := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); !errors.Is(cancelErr, ErrInvocationOutcomeUnknown) {
		t.Fatalf("late Cancel() error = %v", cancelErr)
	}
	close(barrier.release)
	if invokeErr := <-invokeDone; !errors.Is(invokeErr, errRuntimeHandlerFailure) {
		t.Fatalf("Invoke() error = %v", invokeErr)
	}
	record, found, err := commandRuntime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationFailed || record.Cancellation != nil {
		t.Fatalf("failed late-cancel record = %#v, found %v, error %v", record, found, err)
	}
}

func TestRuntimeCancellationDoesNotRewritePostSignalFailure(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.cancel-then-fail.v1"}),
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newCancelThenFailHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "cancel-then-fail")

	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := commandRuntime.Invoke(t.Context(), plan)
		invokeDone <- invokeErr
	}()
	<-handler.started
	if _, cancelErr := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); cancelErr != nil {
		t.Fatal(cancelErr)
	}
	if invokeErr := <-invokeDone; !errors.Is(invokeErr, errRuntimeHandlerFailure) {
		t.Fatalf("Invoke() error = %v", invokeErr)
	}
	record, found, err := commandRuntime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationFailed || record.Cancellation != nil {
		t.Fatalf("post-signal failure record = %#v, found %v, error %v", record, found, err)
	}
}

func TestRuntimeExplicitCancellationDoesNotOverrideParentCause(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.parent-cancel.v1"}),
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newParentCancellationHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "parent-cancel-wins")
	parentCtx, cancelParent := context.WithCancel(t.Context())

	invokeDone := make(chan error, 1)
	go func() {
		_, invokeErr := commandRuntime.Invoke(parentCtx, plan)
		invokeDone <- invokeErr
	}()
	<-handler.started
	cancelParent()
	<-handler.canceled
	if _, cancelErr := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); !errors.Is(cancelErr, ErrInvocationOutcomeUnknown) {
		t.Fatalf("explicit Cancel() after parent cancellation error = %v", cancelErr)
	}
	close(handler.release)
	if invokeErr := <-invokeDone; errors.Is(invokeErr, ErrInvocationCanceled) {
		t.Fatalf("parent cancellation was rewritten as explicit cancellation: %v", invokeErr)
	}
	record, found, err := commandRuntime.Invocation(plan.InvocationID)
	if err != nil || !found || record.State != nodes.InvocationFailed || record.Cancellation != nil {
		t.Fatalf("parent-canceled record = %#v, found %v, error %v", record, found, err)
	}
}

func TestRuntimeRejectsUnsupportedCancellationWithoutMutation(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"node.info.v1"}),
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := testRuntimePlan(t, commandRuntime, "node.info.v1", json.RawMessage(`{}`))
	if _, _, err := ledger.Accept(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); !errors.Is(err, ErrCancellationUnsupported) {
		t.Fatalf("Cancel() error = %v", err)
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationAccepted || record.Cancellation != nil {
		t.Fatalf("unsupported cancellation record = %#v, found %v", record, found)
	}
}

func TestRuntimeDoesNotClaimCancellationWithoutActiveOwner(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	commandRuntime, err := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.block.v1"}),
		ledger,
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := newRuntimeBlockingHandler()
	descriptor := handler.descriptor()
	commandRuntime.handlers[descriptor.Name] = handler
	commandRuntime.catalog.Commands = append(commandRuntime.catalog.Commands, descriptor)
	plan := testTransportPlan(t, commandRuntime, descriptor, "ownerless")
	if _, _, err := ledger.Accept(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkRunning(plan.InvocationID); err != nil {
		t.Fatal(err)
	}
	if _, err := commandRuntime.Cancel(nodes.InvocationCancelRequest{
		InvocationID: plan.InvocationID,
	}); !errors.Is(err, ErrInvocationOutcomeUnknown) {
		t.Fatalf("Cancel() error = %v", err)
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationRunning || record.Cancellation != nil {
		t.Fatalf("ownerless cancellation record = %#v, found %v", record, found)
	}
}

type runtimeBlockingHandler struct {
	started chan struct{}
}

type cancellationTransitionBarrier struct {
	invocationStore
	entered chan struct{}
	release chan struct{}
}

type failureTransitionBarrier struct {
	invocationStore
	entered chan struct{}
	release chan struct{}
}

func (barrier *failureTransitionBarrier) CompleteFailure(
	invocationID string,
	failure nodes.InvocationFailure,
) (nodes.InvocationRecord, error) {
	close(barrier.entered)
	<-barrier.release
	return barrier.invocationStore.CompleteFailure(invocationID, failure)
}

var errRuntimeHandlerFailure = errors.New("runtime handler failed")

type runtimeFailingHandler struct{}

func (runtimeFailingHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:           "test.fail.v1",
		InputSchema:    json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
	}
}

func (runtimeFailingHandler) execute(context.Context, json.RawMessage) (any, error) {
	return nil, errRuntimeHandlerFailure
}

type cancelThenFailHandler struct {
	started chan struct{}
}

type parentCancellationHandler struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func newParentCancellationHandler() *parentCancellationHandler {
	return &parentCancellationHandler{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (*parentCancellationHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:           "test.parent-cancel.v1",
		InputSchema:    json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
	}
}

func (handler *parentCancellationHandler) execute(
	ctx context.Context,
	_ json.RawMessage,
) (any, error) {
	close(handler.started)
	<-ctx.Done()
	close(handler.canceled)
	<-handler.release
	return nil, fmt.Errorf("%w: %v", errCommandCancellationConfirmed, ctx.Err())
}

func newCancelThenFailHandler() *cancelThenFailHandler {
	return &cancelThenFailHandler{started: make(chan struct{})}
}

func (*cancelThenFailHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:           "test.cancel-then-fail.v1",
		InputSchema:    json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
	}
}

func (handler *cancelThenFailHandler) execute(
	ctx context.Context,
	_ json.RawMessage,
) (any, error) {
	close(handler.started)
	<-ctx.Done()
	return nil, errRuntimeHandlerFailure
}

func (barrier *cancellationTransitionBarrier) RequestCancellation(
	invocationID string,
) (nodes.InvocationRecord, error) {
	close(barrier.entered)
	<-barrier.release
	return barrier.invocationStore.RequestCancellation(invocationID)
}

func newRuntimeBlockingHandler() *runtimeBlockingHandler {
	return &runtimeBlockingHandler{started: make(chan struct{}, 1)}
}

func (*runtimeBlockingHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:           "test.block.v1",
		InputSchema:    json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
	}
}

func (handler *runtimeBlockingHandler) execute(
	ctx context.Context,
	_ json.RawMessage,
) (any, error) {
	handler.started <- struct{}{}
	<-ctx.Done()
	return nil, fmt.Errorf("%w: %v", errCommandCancellationConfirmed, ctx.Err())
}

type ignoringCancellationHandler struct {
	started chan struct{}
	release chan struct{}
}

func newIgnoringCancellationHandler() *ignoringCancellationHandler {
	return &ignoringCancellationHandler{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (*ignoringCancellationHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:           "test.ignore-cancel.v1",
		InputSchema:    json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Risk:           nodes.RiskRead,
		SupportsCancel: true,
	}
}

func (handler *ignoringCancellationHandler) execute(
	context.Context,
	json.RawMessage,
) (any, error) {
	close(handler.started)
	<-handler.release
	return map[string]bool{"ok": true}, nil
}

type countingHandler struct {
	executions int
}

func (*countingHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:         "test.count.v1",
		InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         nodes.RiskRead,
	}
}

func (handler *countingHandler) execute(context.Context, json.RawMessage) (any, error) {
	handler.executions++
	return map[string]int{"executions": handler.executions}, nil
}

func TestRuntimePersistsOutputEncodingFailure(t *testing.T) {
	ledger := newMemoryInvocationLedger()
	runtime, newErr := NewRuntime(
		nodes.ID("node_test"),
		"test",
		testRuntimePolicy([]string{"test.invalid-output.v1"}),
		ledger,
	)
	if newErr != nil {
		t.Fatal(newErr)
	}
	handler := invalidOutputHandler{}
	descriptor := handler.descriptor()
	runtime.handlers[descriptor.Name] = handler
	runtime.catalog.Commands = append(runtime.catalog.Commands, descriptor)
	plan := testRuntimePlan(t, runtime, descriptor.Name, json.RawMessage(`{}`))

	if _, err := runtime.Invoke(t.Context(), plan); err == nil {
		t.Fatal("Invoke() accepted an unencodable command result")
	}
	record, found := ledger.Get(plan.InvocationID)
	if !found || record.State != nodes.InvocationFailed || record.Failure == nil ||
		record.Failure.Code != "INVALID_OUTPUT" {
		t.Fatalf("encoding failure record = %#v, found %v", record, found)
	}
}

type invalidOutputHandler struct{}

func (invalidOutputHandler) descriptor() nodes.CommandDescriptor {
	return nodes.CommandDescriptor{
		Name:         "test.invalid-output.v1",
		InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":false}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Risk:         nodes.RiskRead,
	}
}

func (invalidOutputHandler) execute(context.Context, json.RawMessage) (any, error) {
	return make(chan int), nil
}

func testRuntimePolicy(commands []string) nodes.LocalCommandPolicy {
	return nodes.LocalCommandPolicy{
		Revision:          "policy-test",
		AllowedCommands:   commands,
		MaximumRisk:       nodes.RiskRead,
		MaxTimeoutSeconds: 30,
		MaxOutputBytes:    64 * 1024,
	}
}

func testRuntimePlan(
	t *testing.T,
	runtime *Runtime,
	command string,
	input json.RawMessage,
) nodes.ExecutionPlan {
	return testRuntimePlanAt(t, runtime, command, input, time.Now(), time.Minute)
}

func testRuntimePlanAt(
	t *testing.T,
	runtime *Runtime,
	command string,
	input json.RawMessage,
	preparedAt time.Time,
	ttl time.Duration,
) nodes.ExecutionPlan {
	t.Helper()
	catalog := runtime.Catalog()
	catalogHash, err := catalog.Hash()
	if err != nil {
		t.Fatal(err)
	}
	var descriptor nodes.CommandDescriptor
	for _, candidate := range catalog.Commands {
		if candidate.Name == command {
			descriptor = candidate
			break
		}
	}
	plan, err := nodes.PrepareExecutionPlan(nodes.InvocationRequest{
		InvocationID:     "inv_" + strings.ReplaceAll(command, ".", "_"),
		IdempotencyKey:   "idem_" + strings.ReplaceAll(command, ".", "_"),
		NodeID:           runtime.nodeID,
		CatalogHash:      catalogHash,
		Command:          command,
		Input:            input,
		AgentID:          "agent_test",
		SessionID:        "session_test",
		ActorID:          "actor_test",
		TimeoutSeconds:   5,
		OutputLimitBytes: 4096,
	}, descriptor, LocalExecutor, runtime.policy.Revision, preparedAt, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
