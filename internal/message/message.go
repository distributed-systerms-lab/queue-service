// Package message defines the transport-independent message envelope.
package message

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"
)

const MaxPayloadSize = 1 << 20 // One MiB, excluding envelope metadata.

var queuePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

type Message struct {
	ID            string
	Type          string
	Payload       []byte
	CorrelationID string
	CausationID   string
}

func ValidateQueue(name string) error {
	if !queuePattern.MatchString(name) {
		return errors.New("queue must contain 1 to 64 letters, digits, underscores or hyphens and start with a letter or digit")
	}
	return nil
}

func (m Message) Validate() error {
	for _, field := range []struct {
		name     string
		value    string
		required bool
	}{
		{"id", m.ID, true},
		{"type", m.Type, true},
		{"correlation_id", m.CorrelationID, false},
		{"causation_id", m.CausationID, false},
	} {
		if field.required && field.value == "" {
			return fmt.Errorf("%s is required", field.name)
		}
		if len(field.value) > 255 || !utf8.ValidString(field.value) {
			return fmt.Errorf("%s must be valid UTF-8 and at most 255 bytes", field.name)
		}
	}
	if len(m.Payload) > MaxPayloadSize {
		return fmt.Errorf("payload exceeds %d bytes", MaxPayloadSize)
	}
	return nil
}

// Fingerprint detects accidental reuse of an ID with a different envelope.
// Length prefixes keep adjacent fields unambiguous, including binary payloads.
func (m Message) Fingerprint() [sha256.Size]byte {
	digest := sha256.New()
	for _, field := range [][]byte{[]byte(m.Type), m.Payload, []byte(m.CorrelationID), []byte(m.CausationID)} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write(field)
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], digest.Sum(nil))
	return fingerprint
}
