package main

// Algorithm B of Dolev, Klawe and Rodeh (1982), Section 3.2.
//
// Both functions below are pure: they get a copy of the state of ONE process
// (and, for handleB, ONE incoming message) and return the new state together
// with the messages this process sends to its right neighbour.
// Goroutines, channels and logging live in network.go.

// startB is step B1 of phase 0: every process starts active and sends its id
// with counter 2^0 = 1.
func startB(s State) (State, []Message) {
	s.Max, s.Phase, s.Status = s.ID, 0, Active
	return s, []Message{newM1(s.Max, s.Phase, 1)}
}

// handleB handles one incoming message (steps B1-B3 of the paper).
func handleB(s State, m Message) (State, []Message) {
	if s.Done {
		return s, nil
	}
	if m.Kind == LeaderMsg {
		return handleAnnouncement(s, m)
	}
	// our own id came back in an M1, so it went
	// around the whole ring and is the maximum.
	
	if m.Kind == M1 && m.Value == s.ID {
		s.Status, s.KnownLeader = Leader, s.ID
		return s, []Message{{Kind: LeaderMsg, Value: s.ID}}
	}

	switch s.Status {
	case Active: // B1: we sent our M1 and now receive the M1 of the active process on the left
		if m.Kind == M2 {
			panic("an active process received M2: impossible by the paper")
		}
		if m.Value > s.Max { // the left value is bigger: it may move to us,
			s.Max, s.Status = m.Value, Waiting // wait for its M2 to confirm
			return s, nil
		}
		s.Status = Passive                // our value beat the left one: hand it on as M2,
		return s, []Message{newM2(s.Max)} // a waiting process on the right will take it

	case Waiting:
		if m.Kind == M2 { // B2.1: confirmation, we represent max(v) in the next phase
			if m.Value != s.Max {
				panic("a waiting process received an M2 with a foreign value")
			}
			s.Phase, s.Status = s.Phase+1, Active
			return s, []Message{newM1(s.Max, s.Phase, 1<<s.Phase)} // B1 with counter 2^phase
		}
		s.Status = Passive // B2.2: an M1 came first, so the M2 will never come
		return passiveB(s, m)

	default: // Passive
		return passiveB(s, m)
	}
}

// passiveB is step B3: a passive process relays messages, but uses max(v)
// and the counter to stop hopeless M2s and to become a candidate itself.
func passiveB(s State, m Message) (State, []Message) {
	if m.Kind == M2 { // B3.2
		if m.Value < s.Max {
			return s, nil // we have seen a bigger value: this one is not the maximum, swallow it
		}
		return s, []Message{m}
	}
	// B3.1: an M1 passes by
	if m.Value >= s.Max && m.Counter >= 1 {
		s.Max = m.Value
		if m.Counter > 1 {
			return s, []Message{newM1(m.Value, m.Phase, m.Counter-1)}
		}
		// counter == 1: we are exactly 2^phase steps from the sender,
		// so we become a candidate for this value and stop the counter
		s.Status, s.Phase = Waiting, m.Phase
		return s, []Message{newM1(m.Value, m.Phase, 0)}
	}
	return s, []Message{newM1(m.Value, m.Phase, 0)} // counter 0: plain relay from here on
}

// handleAnnouncement spreads the result after the election (shared by A and B):
// every process learns the leader and stops; the message stops at the leader.
func handleAnnouncement(s State, m Message) (State, []Message) {
	s.Done = true
	if s.Status == Leader {
		return s, nil // the announcement went all the way around
	}
	s.KnownLeader = m.Value
	return s, []Message{m}
}
