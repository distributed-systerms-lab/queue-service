package config

import "testing"

func TestLoad(t *testing.T) {
	t.Setenv("QUEUES", "tasks, events,tasks")
	t.Setenv("QUEUE_NAME", "events")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Queues) != 2 || c.Queue != "events" {
		t.Fatalf("unexpected queues: %+v", c)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct{ name, value string }{
		{"CONCURRENCY", "0"}, {"MAX_RETRIES", "-1"}, {"MAX_RETRIES", "101"},
		{"OPERATION_TIMEOUT", "0s"}, {"RETRY_DELAY", "-1s"}, {"ACKNOWLEDGMENT_DELAY", "2m"},
		{"QUEUES", "tasks,"}, {"QUEUE_NAME", "unknown"}, {"MAX_QUEUE_LENGTH", "x"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("QUEUES", "tasks")
			t.Setenv("QUEUE_NAME", "tasks")
			t.Setenv(test.name, test.value)
			if _, err := Load(); err == nil {
				t.Fatalf("accepted %s=%q", test.name, test.value)
			}
		})
	}
}
