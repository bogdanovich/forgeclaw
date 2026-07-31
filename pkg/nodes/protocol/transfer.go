package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"regexp"
)

const (
	TransferProtocolVersion  = 1
	MaxTransferChunkBytes    = 256 * 1024
	MaxTransferMetadataBytes = 16 * 1024
	MaxTransferFileBytes     = 1024 * 1024 * 1024

	transferHeaderBytes = 64
)

var (
	ErrInvalidTransferFrame = errors.New("invalid node transfer frame")

	transferMagic             = [4]byte{'M', 'C', 'F', 'T'}
	transferIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
)

type TransferFrameType uint8

const (
	TransferFramePrepare TransferFrameType = iota + 1
	TransferFrameAccept
	TransferFrameDeny
	TransferFrameChunk
	TransferFrameAck
	TransferFrameCommit
	TransferFrameCommitted
	TransferFrameCancel
	TransferFrameStatus
	TransferFrameFailure
)

type TransferDirection uint8

const (
	TransferUpload TransferDirection = iota + 1
	TransferDownload
)

// TransferFrame is the bounded binary frame carried only inside one
// authenticated node WebSocket generation. The live peer generation is the
// session binding; a frame is never accepted before admission or routed across
// peer replacement.
type TransferFrame struct {
	Type           TransferFrameType
	Direction      TransferDirection
	TransferID     string
	PolicyRevision string
	Sequence       uint64
	TotalSize      uint64
	SHA256         [32]byte
	Payload        []byte
}

func EncodeTransferFrame(frame TransferFrame) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, err
	}
	totalLength := transferHeaderBytes +
		len(frame.TransferID) +
		len(frame.PolicyRevision) +
		len(frame.Payload)
	data := make([]byte, totalLength)
	copy(data[:4], transferMagic[:])
	data[4] = TransferProtocolVersion
	data[5] = byte(frame.Type)
	data[6] = byte(frame.Direction)
	binary.BigEndian.PutUint16(data[8:10], uint16(len(frame.TransferID)))
	binary.BigEndian.PutUint16(data[10:12], uint16(len(frame.PolicyRevision)))
	binary.BigEndian.PutUint64(data[12:20], frame.Sequence)
	binary.BigEndian.PutUint64(data[20:28], frame.TotalSize)
	binary.BigEndian.PutUint32(data[28:32], uint32(len(frame.Payload)))
	copy(data[32:64], frame.SHA256[:])
	offset := transferHeaderBytes
	copy(data[offset:], frame.TransferID)
	offset += len(frame.TransferID)
	copy(data[offset:], frame.PolicyRevision)
	offset += len(frame.PolicyRevision)
	copy(data[offset:], frame.Payload)
	return data, nil
}

func DecodeTransferFrame(data []byte) (TransferFrame, error) {
	if len(data) < transferHeaderBytes ||
		!bytes.Equal(data[:4], transferMagic[:]) ||
		data[4] != TransferProtocolVersion ||
		data[7] != 0 {
		return TransferFrame{}, ErrInvalidTransferFrame
	}
	idLength := int(binary.BigEndian.Uint16(data[8:10]))
	revisionLength := int(binary.BigEndian.Uint16(data[10:12]))
	payloadLength := int(binary.BigEndian.Uint32(data[28:32]))
	if idLength <= 0 || idLength > MaxIDLength ||
		revisionLength <= 0 || revisionLength > MaxIDLength ||
		payloadLength < 0 || payloadLength > MaxTransferChunkBytes {
		return TransferFrame{}, ErrInvalidTransferFrame
	}
	totalLength := transferHeaderBytes + idLength + revisionLength + payloadLength
	if totalLength != len(data) {
		return TransferFrame{}, ErrInvalidTransferFrame
	}
	frame := TransferFrame{
		Type:       TransferFrameType(data[5]),
		Direction:  TransferDirection(data[6]),
		Sequence:   binary.BigEndian.Uint64(data[12:20]),
		TotalSize:  binary.BigEndian.Uint64(data[20:28]),
		Payload:    append([]byte(nil), data[transferHeaderBytes+idLength+revisionLength:]...),
		TransferID: string(data[transferHeaderBytes : transferHeaderBytes+idLength]),
		PolicyRevision: string(
			data[transferHeaderBytes+idLength : transferHeaderBytes+idLength+revisionLength],
		),
	}
	copy(frame.SHA256[:], data[32:64])
	if err := frame.Validate(); err != nil {
		return TransferFrame{}, err
	}
	return frame, nil
}

func (frame TransferFrame) Validate() error {
	if !validTransferIdentifier(frame.TransferID) ||
		!validTransferIdentifier(frame.PolicyRevision) ||
		frame.TotalSize > MaxTransferFileBytes ||
		isZeroTransferDigest(frame.SHA256) {
		return ErrInvalidTransferFrame
	}
	switch frame.Direction {
	case TransferUpload, TransferDownload:
	default:
		return ErrInvalidTransferFrame
	}
	switch frame.Type {
	case TransferFramePrepare:
		if frame.Sequence != 0 || len(frame.Payload) == 0 ||
			len(frame.Payload) > MaxTransferMetadataBytes {
			return ErrInvalidTransferFrame
		}
	case TransferFrameChunk:
		if frame.Sequence == 0 || len(frame.Payload) == 0 ||
			len(frame.Payload) > MaxTransferChunkBytes ||
			uint64(len(frame.Payload)) > frame.TotalSize {
			return ErrInvalidTransferFrame
		}
	case TransferFrameAck:
		if frame.Sequence == 0 || len(frame.Payload) != 0 {
			return ErrInvalidTransferFrame
		}
	case TransferFrameAccept,
		TransferFrameDeny,
		TransferFrameCommit,
		TransferFrameCommitted,
		TransferFrameCancel,
		TransferFrameStatus,
		TransferFrameFailure:
		if frame.Sequence != 0 || len(frame.Payload) > MaxTransferMetadataBytes {
			return ErrInvalidTransferFrame
		}
	default:
		return ErrInvalidTransferFrame
	}
	return nil
}

func (frame TransferFrame) SameBinding(other TransferFrame) bool {
	return frame.Direction == other.Direction &&
		frame.TransferID == other.TransferID &&
		frame.PolicyRevision == other.PolicyRevision &&
		frame.TotalSize == other.TotalSize &&
		bytes.Equal(frame.SHA256[:], other.SHA256[:])
}

func validTransferIdentifier(value string) bool {
	return len(value) > 0 &&
		len(value) <= MaxIDLength &&
		transferIdentifierPattern.MatchString(value)
}

func isZeroTransferDigest(digest [32]byte) bool {
	var zero [32]byte
	return bytes.Equal(digest[:], zero[:])
}
