package event

import "testing"

func TestParse(t *testing.T) {
	e, err := Parse([]byte(`{"event":"run.completed","workflowId":"w1","runId":"r1","step":2}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.WorkflowID != "w1" || e.Event != "run.completed" || e.Step != 2 {
		t.Fatalf("unexpected event: %+v", e)
	}
	if !e.IsCompletion() {
		t.Fatal("expected completion")
	}
}

func TestParseInvalid(t *testing.T) {
	if _, err := Parse([]byte("not json")); err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestIsCompletionFalse(t *testing.T) {
	if (Event{Event: "run.advanced"}).IsCompletion() {
		t.Fatal("run.advanced is not a completion")
	}
}
