package companion

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/bogdanovich/mintclaw/pkg/nodes"
	"github.com/bogdanovich/mintclaw/pkg/nodes/internal/jsonstrict"
	"github.com/bogdanovich/mintclaw/pkg/nodes/protocol"
)

const (
	FileHelperProtocolVersion = 1
	MaxFileHelperMessageBytes = 512 * 1024

	fileHelperHeaderBytes = 12
)

type fileHelperMessageKind uint8

const (
	fileHelperSnapshotRequest fileHelperMessageKind = iota + 1
	fileHelperSnapshotResponse
	fileHelperTransferRequest
	fileHelperTransferResponse
	fileHelperErrorResponse
)

var (
	fileHelperMagic       = [4]byte{'M', 'C', 'F', 'H'}
	fileHelperCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
)

type fileHelperSnapshot struct {
	Profile         nodes.FileProfileDescriptor `json:"profile"`
	AuthorityDigest string                      `json:"authority_digest"`
	ServiceDigest   string                      `json:"service_digest"`
}

type fileHelperMessage struct {
	Kind    fileHelperMessageKind
	Payload []byte
}

type fileHelperTransfer struct {
	ServiceDigest string
	ProfileAlias  string
	Frame         protocol.TransferFrame
}

func readFileHelperMessage(reader io.Reader) (fileHelperMessage, error) {
	var header [fileHelperHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return fileHelperMessage{}, err
	}
	if !bytes.Equal(header[:4], fileHelperMagic[:]) ||
		header[4] != FileHelperProtocolVersion ||
		header[6] != 0 ||
		header[7] != 0 {
		return fileHelperMessage{}, errors.New("file helper message header is invalid")
	}
	length := binary.BigEndian.Uint32(header[8:12])
	if length > MaxFileHelperMessageBytes {
		return fileHelperMessage{}, errors.New("file helper message exceeds bounds")
	}
	message := fileHelperMessage{
		Kind:    fileHelperMessageKind(header[5]),
		Payload: make([]byte, int(length)),
	}
	if _, err := io.ReadFull(reader, message.Payload); err != nil {
		return fileHelperMessage{}, err
	}
	if err := message.validate(); err != nil {
		return fileHelperMessage{}, err
	}
	return message, nil
}

func writeFileHelperMessage(writer io.Writer, message fileHelperMessage) error {
	if err := message.validate(); err != nil {
		return err
	}
	if len(message.Payload) > MaxFileHelperMessageBytes {
		return errors.New("file helper message exceeds bounds")
	}
	var header [fileHelperHeaderBytes]byte
	copy(header[:4], fileHelperMagic[:])
	header[4] = FileHelperProtocolVersion
	header[5] = byte(message.Kind)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(message.Payload)))
	if _, err := io.Copy(writer, bytes.NewReader(header[:])); err != nil {
		return err
	}
	if len(message.Payload) == 0 {
		return nil
	}
	_, err := io.Copy(writer, bytes.NewReader(message.Payload))
	return err
}

func (message fileHelperMessage) validate() error {
	switch message.Kind {
	case fileHelperSnapshotRequest:
		if len(message.Payload) != 0 {
			return errors.New("file helper snapshot request carries a payload")
		}
	case fileHelperSnapshotResponse:
		if len(message.Payload) == 0 || len(message.Payload) > MaxFileHelperMessageBytes {
			return errors.New("file helper snapshot response is invalid")
		}
		if _, err := decodeFileHelperSnapshot(message.Payload); err != nil {
			return err
		}
	case fileHelperTransferRequest:
		if _, err := decodeFileHelperTransferRequest(message.Payload); err != nil {
			return errors.New("file helper transfer request is invalid")
		}
	case fileHelperTransferResponse:
		if _, err := protocol.DecodeTransferFrame(message.Payload); err != nil {
			return errors.New("file helper transfer message is invalid")
		}
	case fileHelperErrorResponse:
		if !fileHelperCodePattern.Match(message.Payload) {
			return errors.New("file helper error response is invalid")
		}
	default:
		return errors.New("file helper message kind is unsupported")
	}
	return nil
}

func encodeFileHelperTransferRequest(
	serviceDigest string,
	profileAlias string,
	frame protocol.TransferFrame,
) ([]byte, error) {
	if !validFileHelperDigest(serviceDigest) {
		return nil, errors.New("file helper service digest is invalid")
	}
	if err := (nodes.Alias(profileAlias)).Validate(); err != nil {
		return nil, errors.New("file helper profile alias is invalid")
	}
	encoded, err := protocol.EncodeTransferFrame(frame)
	if err != nil {
		return nil, err
	}
	if len(profileAlias) > 0xffff ||
		len(encoded)+len(profileAlias)+2+sha256.Size*2 > MaxFileHelperMessageBytes {
		return nil, errors.New("file helper transfer request exceeds bounds")
	}
	payload := make([]byte, len(encoded)+len(profileAlias)+2+sha256.Size*2)
	binary.BigEndian.PutUint16(payload[:2], uint16(len(profileAlias)))
	copy(payload[2:], profileAlias)
	digestOffset := 2 + len(profileAlias)
	copy(payload[digestOffset:], serviceDigest)
	copy(payload[digestOffset+sha256.Size*2:], encoded)
	return payload, nil
}

func decodeFileHelperTransferRequest(payload []byte) (fileHelperTransfer, error) {
	if len(payload) < 3 {
		return fileHelperTransfer{}, errors.New("file helper transfer request is truncated")
	}
	aliasLength := int(binary.BigEndian.Uint16(payload[:2]))
	if aliasLength <= 0 || 2+aliasLength+sha256.Size*2 >= len(payload) {
		return fileHelperTransfer{}, errors.New("file helper transfer request alias length is invalid")
	}
	alias := string(payload[2 : 2+aliasLength])
	if err := (nodes.Alias(alias)).Validate(); err != nil {
		return fileHelperTransfer{}, errors.New("file helper transfer request alias is invalid")
	}
	digestOffset := 2 + aliasLength
	serviceDigest := string(payload[digestOffset : digestOffset+sha256.Size*2])
	if !validFileHelperDigest(serviceDigest) {
		return fileHelperTransfer{}, errors.New("file helper transfer request digest is invalid")
	}
	frame, err := protocol.DecodeTransferFrame(payload[digestOffset+sha256.Size*2:])
	if err != nil {
		return fileHelperTransfer{}, err
	}
	return fileHelperTransfer{
		ServiceDigest: serviceDigest,
		ProfileAlias:  alias,
		Frame:         frame,
	}, nil
}

func encodeFileHelperSnapshot(
	descriptors []nodes.CommandDescriptor,
	serviceDigest string,
) ([]byte, error) {
	if err := validateFileHelperDescriptors(descriptors); err != nil {
		return nil, err
	}
	if !validFileHelperDigest(serviceDigest) {
		return nil, errors.New("file helper service digest is invalid")
	}
	combinedAuthority, err := combineFileHelperAuthority(
		descriptors[0].ModelContract.AuthorityDigest,
		serviceDigest,
	)
	if err != nil {
		return nil, err
	}
	snapshot := fileHelperSnapshot{
		Profile:         descriptors[0].FileProfiles[0],
		AuthorityDigest: combinedAuthority,
		ServiceDigest:   serviceDigest,
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("encode file helper snapshot: %w", err)
	}
	if len(data) == 0 || len(data) > MaxFileHelperMessageBytes {
		return nil, errors.New("file helper snapshot exceeds bounds")
	}
	return data, nil
}

func decodeFileHelperSnapshot(data []byte) (fileHelperSnapshot, error) {
	if len(data) == 0 || len(data) > MaxFileHelperMessageBytes {
		return fileHelperSnapshot{}, errors.New("file helper snapshot exceeds bounds")
	}
	if _, err := jsonstrict.Decode(data); err != nil {
		return fileHelperSnapshot{}, fmt.Errorf("validate file helper snapshot: %w", err)
	}
	var snapshot fileHelperSnapshot
	if err := decodeStrictJSON(data, &snapshot); err != nil {
		return fileHelperSnapshot{}, fmt.Errorf("decode file helper snapshot: %w", err)
	}
	if err := snapshot.validate(); err != nil {
		return fileHelperSnapshot{}, err
	}
	return snapshot, nil
}

func (snapshot fileHelperSnapshot) validate() error {
	if err := snapshot.Profile.Validate(); err != nil {
		return err
	}
	if !validFileHelperDigest(snapshot.AuthorityDigest) ||
		!validFileHelperDigest(snapshot.ServiceDigest) {
		return errors.New("file helper snapshot authority digest is invalid")
	}
	return nil
}

func validFileHelperDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func combineFileHelperAuthority(profileDigest, serviceDigest string) (string, error) {
	if !validFileHelperDigest(profileDigest) || !validFileHelperDigest(serviceDigest) {
		return "", errors.New("file helper authority digest is invalid")
	}
	data, err := json.Marshal([]string{profileDigest, serviceDigest})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func (snapshot fileHelperSnapshot) descriptors() ([]nodes.CommandDescriptor, error) {
	if err := snapshot.validate(); err != nil {
		return nil, err
	}
	contract := &nodes.CommandModelContract{
		Availability:      nodes.ModelUnavailable,
		TimeoutSecondsMax: nodes.MaxInvocationTimeout,
		OutputBytesMax:    nodes.MaxInvocationOutput,
		ResultKind:        "json",
		AuthorityDigest:   snapshot.AuthorityDigest,
		Constraints: nodes.CommandModelConstraints{
			ProfileAliases: []string{snapshot.Profile.Alias},
		},
		Guidance: []string{},
		Examples: []json.RawMessage{},
	}
	profiles := []nodes.FileProfileDescriptor{snapshot.Profile}
	descriptors := []nodes.CommandDescriptor{
		fileCapabilityDescriptor("file.info.v1", nodes.RiskRead, contract, profiles),
		fileCapabilityDescriptor("file.download.v1", nodes.RiskRead, contract, profiles),
		fileCapabilityDescriptor("file.upload.v1", nodes.RiskWrite, contract, profiles),
	}
	if err := validateFileHelperDescriptors(descriptors); err != nil {
		return nil, err
	}
	return descriptors, nil
}

func validateFileHelperDescriptors(descriptors []nodes.CommandDescriptor) error {
	if len(descriptors) != 3 {
		return errors.New("file helper snapshot must contain three file capabilities")
	}
	profiles := map[string]struct{}{}
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("validate file helper descriptor: %w", err)
		}
		switch descriptor.Name {
		case "file.info.v1", "file.download.v1", "file.upload.v1":
		default:
			return errors.New("file helper snapshot contains a non-file capability")
		}
		if len(descriptor.FileProfiles) != 1 {
			return errors.New("file helper must project exactly one profile")
		}
		if descriptor.ModelContract == nil || descriptor.ModelContract.AuthorityDigest == "" {
			return errors.New("file helper capability lacks authority binding")
		}
		profile := descriptor.FileProfiles[0]
		profiles[profile.Alias+"\x00"+profile.Revision] = struct{}{}
	}
	if len(profiles) != 1 {
		return errors.New("file helper capabilities disagree on profile identity")
	}
	return nil
}
