package message

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Message)
		valid  bool
	}{
		{"valid envelope", func(*Message) {}, true},
		{"empty payload", func(m *Message) { m.Payload = nil }, true},
		{"missing ID", func(m *Message) { m.ID = "" }, false},
		{"missing type", func(m *Message) { m.Type = "" }, false},
		{"oversized payload", func(m *Message) { m.Payload = make([]byte, MaxPayloadSize+1) }, false},
		{"boundary payload", func(m *Message) { m.Payload = make([]byte, MaxPayloadSize) }, true},
		{"oversized metadata", func(m *Message) { m.CorrelationID = strings.Repeat("x", 256) }, false},
		{"invalid UTF-8", func(m *Message) { m.Type = string([]byte{0xff}) }, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := Message{ID: "example", Type: "lab.echo", Payload: []byte("hello")}
			test.change(&item)
			if err := item.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, test.valid)
			}
		})
	}
}

func TestFingerprintUsesEveryEnvelopeField(t *testing.T) {
	original := Message{ID: "example", Type: "lab.echo", Payload: []byte("hello"), CorrelationID: "run", CausationID: "parent"}
	for _, change := range []func(*Message){
		func(m *Message) { m.Type = "other" },
		func(m *Message) { m.Payload = []byte("other") },
		func(m *Message) { m.CorrelationID = "other" },
		func(m *Message) { m.CausationID = "other" },
	} {
		changed := original
		change(&changed)
		if original.Fingerprint() == changed.Fingerprint() {
			t.Fatal("different envelopes have identical fingerprints")
		}
	}
	left := Message{Type: "ab", Payload: []byte("c")}
	right := Message{Type: "a", Payload: []byte("bc")}
	if left.Fingerprint() == right.Fingerprint() {
		t.Fatal("field boundaries are ambiguous")
	}
	if (Message{Type: "lab.echo"}).Fingerprint() != (Message{Type: "lab.echo", Payload: []byte{}}).Fingerprint() {
		t.Fatal("nil and empty payloads differ")
	}
}

func TestValidateQueue(t *testing.T) {
	for _, name := range []string{"tasks", "jobs_2", "Events-1"} {
		if err := ValidateQueue(name); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
	for _, name := range []string{"", "tasks.dead", "amq.internal", "-tasks", "tasks,other", strings.Repeat("x", 65)} {
		if err := ValidateQueue(name); err == nil {
			t.Errorf("accepted invalid queue %q", name)
		}
	}
}
