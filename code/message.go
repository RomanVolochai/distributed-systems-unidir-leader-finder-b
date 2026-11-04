package main

import "fmt"

// Kind is the type of a message.
type Kind int

const (
	M1        Kind = iota + 1 // (1, i, p, c): "this is value i in phase p", c is the hop counter
	M2                        // (2, j): "value j beat its left neighbour" (j = max of the sender)
	LeaderMsg                 // after the election: "the leader is j", travels once around the ring
)

// noCounter marks M1 messages of Algorithm A, which have no hop counter.
const noCounter = -1

// Message is what travels along a link of the ring.
type Message struct {
	Kind    Kind
	Value   int
	Phase   int // M1 only
	Counter int // M1 only 
}

func newM1(value, phase, counter int) Message {
	return Message{Kind: M1, Value: value, Phase: phase, Counter: counter}
}

func newM2(value int) Message {
	return Message{Kind: M2, Value: value}
}

func (m Message) String() string {
	switch m.Kind {
	case M1:
		if m.Counter == noCounter {
			return fmt.Sprintf("M1(value=%d, phase=%d)", m.Value, m.Phase)
		}
		return fmt.Sprintf("M1(value=%d, phase=%d, counter=%d)", m.Value, m.Phase, m.Counter)
	case M2:
		return fmt.Sprintf("M2(value=%d)", m.Value)
	default:
		return fmt.Sprintf("LEADER(%d)", m.Value)
	}
}
