package main

// Algorithm A of Dolev, Klawe and Rodeh (1982), Section 2.1 (the same as
// Peterson's first algorithm). It is here only to compare message counts
// with Algorithm B: run with -algo A.

// startA is step A0: send the own value.
func startA(s State) (State, []Message) {
	s.Max, s.Phase, s.Status = s.ID, 0, Active
	return s, []Message{newM1(s.Max, s.Phase, noCounter)}
}

// handleA handles one incoming message (steps A1 and A2).
func handleA(s State, m Message) (State, []Message) {
	if s.Done {
		return s, nil
	}
	if m.Kind == LeaderMsg {
		return handleAnnouncement(s, m)
	}
	if s.Status == Passive {
		return s, []Message{m} // passive processes relay everything unchanged
	}

	if m.Kind == M1 { // A1: value of the nearest active process on the left
		if m.Value == s.Max { // our own value came back: we are the only active process
			s.Status, s.KnownLeader = Leader, s.Max
			return s, []Message{{Kind: LeaderMsg, Value: s.Max}}
		}
		s.Left = m.Value
		return s, []Message{newM2(m.Value)} // tell the right neighbour about it
	}

	// A2: m.Value is the value of the active process two steps to the left
	if s.Left > m.Value && s.Left > s.Max { // the left value is a local maximum: take it
		s.Max, s.Phase = s.Left, s.Phase+1
		return s, []Message{newM1(s.Max, s.Phase, noCounter)}
	}
	s.Status = Passive
	return s, nil
}
