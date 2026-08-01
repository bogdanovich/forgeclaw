package nodes

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/bogdanovich/mintclaw/pkg/config"
	"github.com/bogdanovich/mintclaw/pkg/netbind"
	nodepkg "github.com/bogdanovich/mintclaw/pkg/nodes"
)

const (
	terminalOperatorPath     = "/nodes/v1/terminal/ws"
	terminalOperatorOpenPath = terminalOperatorPath
	terminalSmokeMarker      = "MINTCLAW_PTY_OK"
	terminalLocalEscape      = byte(0x1d) // Ctrl+]
)

var terminalOutputPattern = regexp.MustCompile(
	`MINTCLAW_PTY_OK UID=([0-9]+) SIZE=([0-9]+) ([0-9]+)`,
)

type terminalConfigLoader func() (*config.Config, error)

type terminalOperatorCredentials struct {
	Token  string
	Origin string
}

type terminalOpenOptions struct {
	Target       string
	Profile      string
	WorkingScope string
	Columns      int
	Rows         int
}

type terminalSmokeOptions struct {
	Target       string
	Profile      string
	WorkingScope string
	Columns      int
	Rows         int
	Timeout      time.Duration
	Progress     func(string)
}

func (options terminalSmokeOptions) openOptions() terminalOpenOptions {
	return terminalOpenOptions{
		Target:       options.Target,
		Profile:      options.Profile,
		WorkingScope: options.WorkingScope,
		Columns:      options.Columns,
		Rows:         options.Rows,
	}
}

type terminalSmokeResult struct {
	Target      string `json:"target"`
	Profile     string `json:"profile"`
	TerminalID  string `json:"terminal_id"`
	UID         int    `json:"uid"`
	Rows        int    `json:"rows"`
	Columns     int    `json:"columns"`
	Marker      string `json:"marker"`
	State       string `json:"state"`
	CloseReason string `json:"close_reason,omitempty"`
}

type terminalOpenRequest struct {
	Version      int    `json:"version"`
	SessionID    string `json:"session_id"`
	RequestID    string `json:"request_id"`
	Target       string `json:"target"`
	Profile      string `json:"profile"`
	WorkingScope string `json:"working_scope"`
	Columns      int    `json:"columns"`
	Rows         int    `json:"rows"`
}

type terminalOpenResult struct {
	TerminalID   string `json:"terminal_id"`
	State        string `json:"state"`
	AttachBefore int64  `json:"attach_before"`
}

type terminalOpenError struct {
	Error string `json:"error"`
}

type terminalOperatorControl struct {
	Version        int    `json:"version"`
	Type           string `json:"type"`
	Sequence       uint64 `json:"sequence"`
	IdempotencyKey string `json:"idempotency_key"`
	InputBase64    string `json:"input_base64,omitempty"`
	Columns        int    `json:"columns,omitempty"`
	Rows           int    `json:"rows,omitempty"`
}

type terminalOperatorAttached struct {
	Version    int    `json:"version"`
	Type       string `json:"type"`
	TerminalID string `json:"terminal_id"`
	State      string `json:"state"`
}

type localTerminal interface {
	IsTerminal(int) bool
	GetSize(int) (int, int, error)
	MakeRaw(int) (*term.State, error)
	Restore(int, *term.State) error
}

type systemLocalTerminal struct{}

func (systemLocalTerminal) IsTerminal(fd int) bool { return term.IsTerminal(fd) }
func (systemLocalTerminal) GetSize(fd int) (int, int, error) {
	return term.GetSize(fd)
}
func (systemLocalTerminal) MakeRaw(fd int) (*term.State, error) { return term.MakeRaw(fd) }
func (systemLocalTerminal) Restore(fd int, state *term.State) error {
	return term.Restore(fd, state)
}

type terminalControlAction struct {
	typeName string
	input    []byte
	columns  int
	rows     int
}

type interactiveTerminalClient struct {
	connection *websocket.Conn
	actions    chan terminalControlAction
	writeErr   chan error
	closeOnce  sync.Once
}

func newTerminalCommand(load terminalConfigLoader) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "terminal",
		Short: "Test and operate attached node terminals",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(newTerminalOpenCommand(load), newTerminalSmokeCommand(load))
	return cmd
}

func newTerminalOpenCommand(load terminalConfigLoader) *cobra.Command {
	options := terminalOpenOptions{}
	cmd := &cobra.Command{
		Use:   "open",
		Short: "Open an interactive terminal on a connected node",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			resizeSignals := make(chan os.Signal, 1)
			stopResize := notifyTerminalResize(resizeSignals)
			defer stopResize()
			stopSignals := make(chan os.Signal, 1)
			signal.Notify(stopSignals, terminalTerminationSignals()...)
			defer signal.Stop(stopSignals)
			return runInteractiveTerminal(
				cmd.Context(),
				cfg,
				options,
				os.Stdin,
				os.Stdout,
				cmd.ErrOrStderr(),
				systemLocalTerminal{},
				resizeSignals,
				stopSignals,
			)
		},
	}
	cmd.Flags().StringVar(&options.Target, "target", "", "Visible node target name")
	cmd.Flags().StringVar(&options.Profile, "profile", "", "Owner shell profile alias")
	cmd.Flags().StringVar(&options.WorkingScope, "working-scope", "", "Configured working-scope alias")
	_ = cmd.MarkFlagRequired("target")
	_ = cmd.MarkFlagRequired("profile")
	_ = cmd.MarkFlagRequired("working-scope")
	return cmd
}

func newTerminalSmokeCommand(load terminalConfigLoader) *cobra.Command {
	options := terminalSmokeOptions{
		Columns: 100,
		Rows:    31,
		Timeout: 30 * time.Second,
	}
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "smoke",
		Short: "Run a safe end-to-end PTY lifecycle check",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := load()
			if err != nil {
				return err
			}
			if !jsonOutput {
				options.Progress = func(message string) {
					fmt.Fprintln(cmd.ErrOrStderr(), message)
				}
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), options.Timeout)
			defer cancel()
			result, err := runTerminalSmoke(ctx, cfg, options)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Target: %s\n", result.Target)
			fmt.Fprintf(cmd.OutOrStdout(), "Profile: %s\n", result.Profile)
			fmt.Fprintf(cmd.OutOrStdout(), "UID: %d\n", result.UID)
			fmt.Fprintf(cmd.OutOrStdout(), "Size: %dx%d\n", result.Columns, result.Rows)
			fmt.Fprintf(cmd.OutOrStdout(), "Marker: %s\n", result.Marker)
			fmt.Fprintf(cmd.OutOrStdout(), "State: %s\n", result.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&options.Target, "target", "", "Visible node target name")
	cmd.Flags().StringVar(&options.Profile, "profile", "", "Owner shell profile alias")
	cmd.Flags().StringVar(&options.WorkingScope, "working-scope", "", "Configured working-scope alias")
	cmd.Flags().IntVar(&options.Columns, "columns", options.Columns, "PTY columns for the resize check")
	cmd.Flags().IntVar(&options.Rows, "rows", options.Rows, "PTY rows for the resize check")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", options.Timeout, "Overall smoke-test timeout")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Emit stable JSON output")
	_ = cmd.MarkFlagRequired("target")
	_ = cmd.MarkFlagRequired("profile")
	_ = cmd.MarkFlagRequired("working-scope")
	return cmd
}

func runInteractiveTerminal(
	ctx context.Context,
	cfg *config.Config,
	options terminalOpenOptions,
	stdin *os.File,
	stdout io.Writer,
	status io.Writer,
	local localTerminal,
	resizeSignals <-chan os.Signal,
	terminationSignals <-chan os.Signal,
) (returnErr error) {
	if stdin == nil || local == nil || !local.IsTerminal(int(stdin.Fd())) {
		return errors.New("interactive terminal requires a local TTY on stdin")
	}
	columns, rows, err := local.GetSize(int(stdin.Fd()))
	if err != nil {
		return fmt.Errorf("read local terminal size: %w", err)
	}
	options.Columns, options.Rows = columns, rows
	lastColumns, lastRows := columns, rows
	if validationErr := validateTerminalOptions(cfg, options); validationErr != nil {
		return validationErr
	}
	fmt.Fprintf(status, "Opening terminal on %s...\n", options.Target)
	credentials, baseURL, sessionID, opened, err := openOperatorTerminal(ctx, cfg, options)
	if err != nil {
		return err
	}
	fmt.Fprintf(status, "Terminal %s opened; attaching...\n", opened.TerminalID)
	connection, attached, err := attachOperatorTerminal(
		ctx,
		baseURL,
		credentials,
		sessionID,
		opened.TerminalID,
	)
	if err != nil {
		return err
	}
	defer connection.Close()
	if attached.State != "live" {
		return fmt.Errorf("terminal attachment entered unexpected state %q", attached.State)
	}
	fmt.Fprintf(status, "Attached. Press Ctrl+] to close and disconnect.\n")
	previous, err := local.MakeRaw(int(stdin.Fd()))
	if err != nil {
		return fmt.Errorf("enter local raw mode: %w", err)
	}
	defer func() {
		if err := local.Restore(int(stdin.Fd()), previous); err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("restore local terminal: %w", err))
		}
	}()
	client := newInteractiveTerminalClient(connection)
	sessionCtx, cancelSession := context.WithCancel(context.Background())
	defer cancelSession()
	client.startWriter(sessionCtx)
	if err := client.send(ctx, terminalControlAction{
		typeName: "resize",
		columns:  columns,
		rows:     rows,
	}); err != nil {
		return err
	}
	inputClosed := make(chan struct{})
	go readInteractiveTerminalInput(sessionCtx, stdin, client, inputClosed)
	events := make(chan nodepkg.TerminalEvent)
	readErr := make(chan error, 1)
	go readInteractiveTerminalEvents(sessionCtx, connection, events, readErr)
	var cursor uint64
	var closeTimer <-chan time.Time
	ctxDone := ctx.Done()
	for {
		select {
		case <-ctxDone:
			ctxDone = nil
			client.requestClose()
			if closeTimer == nil {
				closeTimer = time.After(5 * time.Second)
			}
		case <-terminationSignals:
			client.requestClose()
			if closeTimer == nil {
				closeTimer = time.After(5 * time.Second)
			}
		case <-inputClosed:
			inputClosed = nil
			client.requestClose()
			if closeTimer == nil {
				closeTimer = time.After(10 * time.Second)
			}
		case <-resizeSignals:
			columns, rows, sizeErr := local.GetSize(int(stdin.Fd()))
			if sizeErr == nil && (columns != lastColumns || rows != lastRows) {
				if sendErr := client.send(sessionCtx, terminalControlAction{
					typeName: "resize",
					columns:  columns,
					rows:     rows,
				}); sendErr != nil {
					return sendErr
				}
				lastColumns, lastRows = columns, rows
			}
		case err := <-client.writeErr:
			return fmt.Errorf("write terminal control: %w", err)
		case err := <-readErr:
			return fmt.Errorf("terminal disconnected before confirmed close: %w", err)
		case <-closeTimer:
			return errors.New("terminal close was not confirmed before timeout")
		case event := <-events:
			if event.TerminalID != opened.TerminalID {
				return errors.New("terminal event identity changed")
			}
			switch event.Type {
			case "output":
				data, decodeErr := base64.StdEncoding.Strict().DecodeString(event.DataBase64)
				if decodeErr != nil || event.Cursor != cursor+uint64(len(data)) {
					return errors.New("terminal returned discontinuous output")
				}
				cursor = event.Cursor
				if _, writeErr := stdout.Write(data); writeErr != nil {
					return fmt.Errorf("write terminal output: %w", writeErr)
				}
			case "closed":
				if !event.TerminationConfirmed {
					return errors.New("remote process-tree termination was not confirmed")
				}
				return nil
			case "unknown", "denied":
				return fmt.Errorf("terminal entered %s state", event.Type)
			}
		}
	}
}

func newInteractiveTerminalClient(
	connection *websocket.Conn,
) *interactiveTerminalClient {
	return &interactiveTerminalClient{
		connection: connection,
		actions:    make(chan terminalControlAction, 32),
		writeErr:   make(chan error, 1),
	}
}

func (client *interactiveTerminalClient) startWriter(ctx context.Context) {
	go func() {
		var sequence uint64
		for {
			select {
			case <-ctx.Done():
				return
			case action := <-client.actions:
				sequence++
				control := terminalOperatorControl{
					Version:        nodepkg.TerminalProtocolVersion,
					Type:           action.typeName,
					Sequence:       sequence,
					IdempotencyKey: fmt.Sprintf("terminal_cli_%s_%d", action.typeName, sequence),
					Columns:        action.columns,
					Rows:           action.rows,
				}
				if len(action.input) != 0 {
					control.InputBase64 = base64.StdEncoding.EncodeToString(action.input)
				}
				if err := client.connection.WriteJSON(control); err != nil {
					select {
					case client.writeErr <- err:
					default:
					}
					return
				}
			}
		}
	}()
}

func (client *interactiveTerminalClient) send(ctx context.Context, action terminalControlAction) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case client.actions <- action:
		return nil
	}
}

func (client *interactiveTerminalClient) requestClose() {
	client.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.send(ctx, terminalControlAction{typeName: "close"})
	})
}

func readInteractiveTerminalInput(
	ctx context.Context,
	stdin io.Reader,
	client *interactiveTerminalClient,
	closed chan<- struct{},
) {
	defer close(closed)
	buffer := make([]byte, 32*1024)
	for {
		count, err := stdin.Read(buffer)
		if count > 0 {
			data := append([]byte(nil), buffer[:count]...)
			if escape := bytes.IndexByte(data, terminalLocalEscape); escape >= 0 {
				if escape > 0 {
					_ = client.send(ctx, terminalControlAction{typeName: "input", input: data[:escape]})
				}
				return
			}
			if sendErr := client.send(ctx, terminalControlAction{typeName: "input", input: data}); sendErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func readInteractiveTerminalEvents(
	ctx context.Context,
	connection *websocket.Conn,
	events chan<- nodepkg.TerminalEvent,
	readErr chan<- error,
) {
	for {
		var event nodepkg.TerminalEvent
		if err := connection.ReadJSON(&event); err != nil {
			readErr <- err
			return
		}
		if _, err := event.Validate(); err != nil {
			readErr <- err
			return
		}
		select {
		case <-ctx.Done():
			return
		case events <- event:
		}
	}
}

func runTerminalSmoke(
	ctx context.Context,
	cfg *config.Config,
	options terminalSmokeOptions,
) (terminalSmokeResult, error) {
	if err := validateTerminalOptions(cfg, options.openOptions()); err != nil {
		return terminalSmokeResult{}, err
	}
	progress := options.Progress
	if progress == nil {
		progress = func(string) {}
	}
	progress("Opening authenticated terminal...")
	credentials, baseURL, sessionID, opened, err := openOperatorTerminal(
		ctx,
		cfg,
		options.openOptions(),
	)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	progress("Attaching operator stream...")
	operator, _, err := attachOperatorTerminal(
		ctx,
		baseURL,
		credentials,
		sessionID,
		opened.TerminalID,
	)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	defer operator.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = operator.SetReadDeadline(deadline)
		_ = operator.SetWriteDeadline(deadline)
	}
	progress("Checking resize, input, output, and UID...")
	if writeErr := operator.WriteJSON(terminalOperatorControl{
		Version:        nodepkg.TerminalProtocolVersion,
		Type:           "resize",
		Sequence:       1,
		IdempotencyKey: "terminal_smoke_resize_1",
		Columns:        options.Columns,
		Rows:           options.Rows,
	}); writeErr != nil {
		return terminalSmokeResult{}, fmt.Errorf("resize terminal: %w", writeErr)
	}
	script := "stty -echo; printf 'MINTCLAW_%s UID=%s SIZE=%s\\n' " +
		"'PTY_OK' \"$(id -u)\" \"$(stty size)\"\n"
	if writeErr := operator.WriteJSON(terminalOperatorControl{
		Version:        nodepkg.TerminalProtocolVersion,
		Type:           "input",
		Sequence:       2,
		IdempotencyKey: "terminal_smoke_input_2",
		InputBase64:    base64.StdEncoding.EncodeToString([]byte(script)),
	}); writeErr != nil {
		return terminalSmokeResult{}, fmt.Errorf("write terminal input: %w", writeErr)
	}
	result, err := readTerminalSmokeOutput(operator, options, opened.TerminalID)
	if err != nil {
		return terminalSmokeResult{}, err
	}
	progress("Confirmed remote process-tree termination.")
	result.Target = options.Target
	result.Profile = options.Profile
	result.TerminalID = opened.TerminalID
	return result, nil
}

func validateTerminalOptions(cfg *config.Config, options terminalOpenOptions) error {
	if cfg == nil {
		return errors.New("terminal requires configuration")
	}
	if !cfg.Nodes.Enabled || !cfg.Nodes.TerminalEnabled {
		return errors.New("terminal requires nodes.enabled and nodes.terminal_enabled")
	}
	if strings.TrimSpace(options.Target) == "" ||
		strings.TrimSpace(options.Profile) == "" ||
		strings.TrimSpace(options.WorkingScope) == "" {
		return errors.New("target, profile, and working scope are required")
	}
	if options.Columns < 20 || options.Columns > 400 || options.Rows < 5 || options.Rows > 200 {
		return errors.New("terminal size is outside supported bounds")
	}
	return nil
}

func openOperatorTerminal(
	ctx context.Context,
	cfg *config.Config,
	options terminalOpenOptions,
) (terminalOperatorCredentials, *url.URL, string, terminalOpenResult, error) {
	credentials, err := mintClawOperatorCredentials(cfg)
	if err != nil {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, err
	}
	baseURL, err := localGatewayURL(cfg)
	if err != nil {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, err
	}
	sessionID := "terminal-operator-" + uuid.NewString()
	requestBody, err := json.Marshal(terminalOpenRequest{
		Version:      nodepkg.TerminalProtocolVersion,
		SessionID:    sessionID,
		RequestID:    uuid.NewString(),
		Target:       options.Target,
		Profile:      options.Profile,
		WorkingScope: options.WorkingScope,
		Columns:      options.Columns,
		Rows:         options.Rows,
	})
	if err != nil {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, err
	}
	endpoint := *baseURL
	endpoint.Path = terminalOperatorOpenPath
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(requestBody))
	if err != nil {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, err
	}
	request.Header.Set("Authorization", "Bearer "+credentials.Token)
	request.Header.Set("Content-Type", "application/json")
	if credentials.Origin != "" {
		request.Header.Set("Origin", credentials.Origin)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, fmt.Errorf("open terminal: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		var failure terminalOpenError
		_ = json.NewDecoder(io.LimitReader(response.Body, 16*1024)).Decode(&failure)
		if failure.Error == "" {
			failure.Error = http.StatusText(response.StatusCode)
		}
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, fmt.Errorf(
			"open terminal: %s",
			failure.Error,
		)
	}
	var result terminalOpenResult
	if err := json.NewDecoder(io.LimitReader(response.Body, 16*1024)).Decode(&result); err != nil {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, fmt.Errorf(
			"decode terminal open response: %w",
			err,
		)
	}
	if result.TerminalID == "" || result.State != string(nodepkg.GatewayTerminalPendingAttach) {
		return terminalOperatorCredentials{}, nil, "", terminalOpenResult{}, errors.New(
			"gateway returned an invalid terminal open response",
		)
	}
	return credentials, baseURL, sessionID, result, nil
}

func attachOperatorTerminal(
	ctx context.Context,
	baseURL *url.URL,
	credentials terminalOperatorCredentials,
	sessionID string,
	terminalID string,
) (*websocket.Conn, terminalOperatorAttached, error) {
	header := http.Header{"Authorization": []string{"Bearer " + credentials.Token}}
	if credentials.Origin != "" {
		header.Set("Origin", credentials.Origin)
	}
	operatorURL := *baseURL
	operatorURL.Scheme = "ws"
	operatorURL.Path = terminalOperatorPath
	query := operatorURL.Query()
	query.Set("session_id", sessionID)
	query.Set("terminal_id", terminalID)
	operatorURL.RawQuery = query.Encode()
	operator, err := dialTerminalWebSocket(ctx, operatorURL.String(), header)
	if err != nil {
		return nil, terminalOperatorAttached{}, fmt.Errorf("attach operator terminal: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = operator.SetReadDeadline(deadline)
		_ = operator.SetWriteDeadline(deadline)
	}
	var attached terminalOperatorAttached
	if err := operator.ReadJSON(&attached); err != nil {
		operator.Close()
		return nil, terminalOperatorAttached{}, fmt.Errorf("read terminal attachment: %w", err)
	}
	_ = operator.SetReadDeadline(time.Time{})
	_ = operator.SetWriteDeadline(time.Time{})
	if attached.Version != nodepkg.TerminalProtocolVersion ||
		attached.Type != "attached" ||
		attached.TerminalID != terminalID ||
		attached.State != "live" {
		operator.Close()
		return nil, terminalOperatorAttached{}, errors.New("gateway returned an invalid terminal attachment")
	}
	return operator, attached, nil
}

func dialTerminalWebSocket(
	ctx context.Context,
	endpoint string,
	header http.Header,
) (*websocket.Conn, error) {
	connection, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, header)
	if err == nil {
		return connection, nil
	}
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	return nil, err
}

func readTerminalSmokeOutput(
	connection *websocket.Conn,
	options terminalSmokeOptions,
	terminalID string,
) (terminalSmokeResult, error) {
	var output strings.Builder
	var cursor uint64
	resizeAcknowledged := false
	closeAcknowledged := false
	closeSent := false
	var parsed terminalSmokeResult
	sendClose := func() error {
		if closeSent || !resizeAcknowledged || parsed.Marker == "" {
			return nil
		}
		if err := connection.WriteJSON(terminalOperatorControl{
			Version:        nodepkg.TerminalProtocolVersion,
			Type:           "close",
			Sequence:       3,
			IdempotencyKey: "terminal_smoke_close_3",
		}); err != nil {
			return fmt.Errorf("close terminal: %w", err)
		}
		closeSent = true
		return nil
	}
	for {
		var event nodepkg.TerminalEvent
		if err := connection.ReadJSON(&event); err != nil {
			return terminalSmokeResult{}, fmt.Errorf("read terminal event: %w", err)
		}
		if _, err := event.Validate(); err != nil {
			return terminalSmokeResult{}, errors.New("terminal returned an invalid event")
		}
		if event.TerminalID != terminalID {
			return terminalSmokeResult{}, errors.New("terminal event identity changed")
		}
		switch event.Type {
		case "output":
			data, err := base64.StdEncoding.Strict().DecodeString(event.DataBase64)
			if err != nil {
				return terminalSmokeResult{}, errors.New("terminal returned invalid base64 output")
			}
			if output.Len()+len(data) > nodepkg.MaxTerminalTransportBuffer {
				return terminalSmokeResult{}, errors.New("terminal smoke output exceeded transport limit")
			}
			cursor += uint64(len(data))
			if event.Cursor != cursor {
				return terminalSmokeResult{}, errors.New("terminal output cursor is discontinuous")
			}
			output.Write(data)
			if !closeSent {
				match := terminalOutputPattern.FindStringSubmatch(output.String())
				if len(match) == 4 {
					uid, uidErr := strconv.Atoi(match[1])
					rows, rowsErr := strconv.Atoi(match[2])
					columns, columnsErr := strconv.Atoi(match[3])
					if uidErr != nil || rowsErr != nil || columnsErr != nil ||
						rows != options.Rows || columns != options.Columns {
						return terminalSmokeResult{}, errors.New("terminal output did not confirm requested size")
					}
					parsed.UID = uid
					parsed.Rows = rows
					parsed.Columns = columns
					parsed.Marker = terminalSmokeMarker
					if err := sendClose(); err != nil {
						return terminalSmokeResult{}, err
					}
				}
			}
		case "ack":
			switch event.AcceptedSequence {
			case 1:
				resizeAcknowledged = true
				if err := sendClose(); err != nil {
					return terminalSmokeResult{}, err
				}
			case 3:
				closeAcknowledged = true
			}
		case "closed":
			if !closeSent || !closeAcknowledged || !event.TerminationConfirmed ||
				event.State != "closed" || event.Reason != "close" {
				return terminalSmokeResult{}, errors.New(
					"terminal closed without confirming the requested close",
				)
			}
			parsed.State = event.State
			parsed.CloseReason = event.Reason
			return parsed, nil
		case "unknown", "denied":
			return terminalSmokeResult{}, fmt.Errorf("terminal entered %s state", event.Type)
		}
	}
}

func mintClawOperatorCredentials(cfg *config.Config) (terminalOperatorCredentials, error) {
	channel := cfg.Channels.GetByType(config.ChannelMintClaw)
	if channel == nil || !channel.Enabled {
		return terminalOperatorCredentials{}, errors.New(
			"enabled MintClaw channel is required for terminal",
		)
	}
	decoded, err := channel.GetDecoded()
	if err != nil {
		return terminalOperatorCredentials{}, fmt.Errorf("decode MintClaw channel: %w", err)
	}
	settings, ok := decoded.(*config.MintClawSettings)
	if !ok || strings.TrimSpace(settings.Token.String()) == "" {
		return terminalOperatorCredentials{}, errors.New("MintClaw channel token is required for terminal")
	}
	origin := ""
	for _, allowed := range settings.AllowOrigins {
		allowed = strings.TrimSpace(allowed)
		if allowed != "" && allowed != "*" {
			origin = allowed
			break
		}
	}
	return terminalOperatorCredentials{
		Token:  strings.TrimSpace(settings.Token.String()),
		Origin: origin,
	}, nil
}

func localGatewayURL(cfg *config.Config) (*url.URL, error) {
	plan, err := netbind.BuildPlan(cfg.Gateway.Host, netbind.DefaultLoopback)
	if err != nil {
		return nil, fmt.Errorf("resolve gateway host: %w", err)
	}
	host := plan.ProbeHost
	if !netbind.IsLoopbackHost(host) {
		return nil, errors.New("terminal must run on the gateway host through a loopback address")
	}
	if cfg.Gateway.Port <= 0 {
		return nil, errors.New("gateway port is required for terminal")
	}
	return &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(host, strconv.Itoa(cfg.Gateway.Port)),
	}, nil
}
