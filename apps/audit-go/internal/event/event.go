package event

import "encoding/json"

// Event is the JSON message value the producer publishes to Kafka.
type Event struct {
	Event      string `json:"event"`
	RunID      string `json:"runId"`
	WorkflowID string `json:"workflowId"`
	Status     string `json:"status"`
	Step       int    `json:"step"`
	At         string `json:"at"`
}

// Parse decodes a Kafka message value into an Event.
func Parse(value []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(value, &e); err != nil {
		return Event{}, err
	}
	return e, nil
}

// IsCompletion reports whether the event marks a run completing.
func (e Event) IsCompletion() bool {
	return e.Event == "run.completed"
}
