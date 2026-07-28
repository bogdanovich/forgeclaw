package companion

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
)

const (
	MaxAuthorityBrokerFrameBytes  = 512 * 1024
	AuthorityBrokerWorkerArgument = "worker"
)

const (
	authorityBrokerActionSnapshot = "snapshot"
	authorityBrokerActionExecute  = "execute"
	authorityBrokerActionTerminal = "terminal.open"
)

type authorityBrokerRequestFrame struct {
	Version  int                        `json:"version"`
	Action   string                     `json:"action"`
	Execute  *ShellBrokerRequest        `json:"execute,omitempty"`
	Terminal *TerminalBrokerOpenRequest `json:"terminal,omitempty"`
}

type authorityBrokerResponseFrame struct {
	Version  int                  `json:"version"`
	OK       bool                 `json:"ok"`
	Code     string               `json:"code,omitempty"`
	Snapshot *ShellBrokerSnapshot `json:"snapshot,omitempty"`
	Result   *ShellBrokerResult   `json:"result,omitempty"`
	Terminal *TerminalBrokerEvent `json:"terminal,omitempty"`
}

func readAuthorityBrokerFrame(reader io.Reader, target any) error {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > MaxAuthorityBrokerFrameBytes {
		return errors.New("authority broker frame length is invalid")
	}
	data := make([]byte, int(length))
	if _, err := io.ReadFull(reader, data); err != nil {
		return err
	}
	if _, err := jsonstrict.Decode(data); err != nil {
		return fmt.Errorf("validate authority broker frame: %w", err)
	}
	if err := decodeStrictJSON(data, target); err != nil {
		return fmt.Errorf("decode authority broker frame: %w", err)
	}
	return nil
}

func writeAuthorityBrokerFrame(writer io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode authority broker frame: %w", err)
	}
	if len(data) == 0 || len(data) > MaxAuthorityBrokerFrameBytes {
		return errors.New("authority broker frame exceeds bounds")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if _, err := io.Copy(writer, bytes.NewReader(header[:])); err != nil {
		return err
	}
	if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
		return err
	}
	return nil
}

func validateAuthorityBrokerRequestFrame(frame authorityBrokerRequestFrame) error {
	if frame.Version != AuthorityBrokerProtocolVersion {
		return errors.New("authority broker protocol version is unsupported")
	}
	switch frame.Action {
	case authorityBrokerActionSnapshot:
		if frame.Execute != nil || frame.Terminal != nil {
			return errors.New("authority broker snapshot request carries execution")
		}
	case authorityBrokerActionExecute:
		if frame.Execute == nil || frame.Terminal != nil {
			return errors.New("authority broker execute request is missing")
		}
	case authorityBrokerActionTerminal:
		if frame.Terminal == nil || frame.Execute != nil {
			return errors.New("authority broker terminal request is missing")
		}
	default:
		return errors.New("authority broker action is unsupported")
	}
	return nil
}
