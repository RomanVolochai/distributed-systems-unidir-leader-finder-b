package main

import "fmt"

// Status of a process. Algorithm B uses Active, Waiting and Passive 
type Status int

const (
	Active  Status = iota // represents a value and starts phases
	Waiting               // candidate for the next phase, waits for an M2
	Passive               // relays messages, but remembers max(v) and filters
	Leader                // knows that it holds the global maximum
)

func (s Status) String() string {
	return [...]string{"ACTIVE", "WAITING", "PASSIVE", "LEADER"}[s]
}

// State is everything one process remembers between two messages.
// It is kept separate from the goroutine: the message handler gets a copy
// of the state and returns the new one.
type State struct {
	ID          int    // id(v): the original number of the process, never changes
	Max         int    // max(v): the value the process represents / the largest it has seen
	Phase       int    // phase(v)
	Status      Status // state(v)
	Left        int    // Algorithm A only: value of the nearest active process on the left
	KnownLeader int    // id announced as the leader (0 = not known yet)
	Done        bool   // the leader announcement has passed: ignore everything
}

func (s State) String() string {
	str := fmt.Sprintf("%s max=%d phase=%d", s.Status, s.Max, s.Phase)
	if s.Done {
		str += fmt.Sprintf(", done (leader %d)", s.KnownLeader)
	}
	return str
}
