package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Delay is waited before every step, so that a run can be followed live.
// It is global and set by the -delay flag (for example -delay 300ms).
var Delay time.Duration

// Verbose turns the per-message log on or off (-quiet switches it off).
var Verbose = true

func logf(format string, args ...any) {
	if Verbose {
		fmt.Printf(format+"\n", args...)
	}
}

// Algorithm is a pair of pure functions: what a process does when it wakes up,
// and what it does with one incoming message.
type Algorithm struct {
	Name   string
	Start  func(State) (State, []Message)
	Handle func(State, Message) (State, []Message)
}

var algorithms = map[string]Algorithm{
	"A": {"A", startA, handleA},
	"B": {"B", startB, handleB},
}

// node is one process of the ring. It runs in its own goroutine.
type node struct {
	pos   int            // position in the ring: P0, P1, ..., P(n-1)
	n     int            // ring size, used only for log messages
	state State          // the state of the process
	inbox []Message      // received but not yet handled messages, in FIFO order
	in    chan Message   // link from the left neighbour (buffer 1)
	out   chan Message   // link to the right neighbour (= the in-link of P(pos+1))
	turn  chan turn      // the scheduler lets this node take one step
	done  chan<- stepLog // the node reports back after the step
}

// turn is a permission to take one step: wake up, or handle the oldest message.
type turn struct {
	step int
	wake bool
}

// stepLog is what the scheduler learns about a finished step (for statistics).
type stepLog struct {
	pos           int
	wake          bool
	handled       Message
	before, after State
	sent          []Message
}

// run is the goroutine of one process. Between steps it keeps accepting
// messages from its left link, so that a sender never waits for long; it
// handles a message only when the scheduler gives it a turn.
func (v *node) run(algo Algorithm, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case m := <-v.in:
			v.inbox = append(v.inbox, m)
		case t, ok := <-v.turn:
			if !ok {
				return // the run is over
			}
			v.step(algo, t)
		}
	}
}

// step takes one step: load state, read one message, compute, send, save state.
func (v *node) step(algo Algorithm, t turn) {
	v.drainLink()
	r := stepLog{pos: v.pos, wake: t.wake, before: v.state}
	if t.wake {
		v.state, r.sent = algo.Start(v.state)
		logf("step %4d  P%d (id %d) wakes up                       -> %v", t.step, v.pos, v.state.ID, v.state)
	} else {
		r.handled, v.inbox = v.inbox[0], v.inbox[1:]
		v.state, r.sent = algo.Handle(v.state, r.handled)
		logf("step %4d  P%d (id %d) got %-36v -> %v", t.step, v.pos, v.state.ID, r.handled, v.state)
		if startsPhase(r.before, v.state) {
			logf("           phase %d starts at P%d with value %d", v.state.Phase, v.pos, v.state.Max)
		}
		if v.state.Status == Leader && r.before.Status != Leader {
			logf("           *** P%d (id %d) is the leader ***", v.pos, v.state.ID)
		}
	}
	for _, m := range r.sent {
		v.out <- m
		logf("           P%d sent %v to P%d", v.pos, m, (v.pos+1)%v.n)
	}
	r.after = v.state
	v.done <- r
}

// drainLink moves a message that is still in the link buffer into the inbox.
func (v *node) drainLink() {
	for {
		select {
		case m := <-v.in:
			v.inbox = append(v.inbox, m)
		default:
			return
		}
	}
}

// Result collects everything we want to know after a run.
type Result struct {
	Algo        string
	IDs         []int
	Final       []State
	Count       map[Kind]int // messages sent, by kind
	Phases      int          // number of phases (phase 0 included)
	Steps       int          // number of steps taken by the scheduler
	M2PerPhase  map[int]int  // M2 messages sent in each phase
	Activations []Activation // who was active at the start of which phase
}

// Activation: process Pos is active at the beginning of phase Phase with value Value.
type Activation struct{ Pos, Phase, Value int }

// Election is the number of messages of the election itself (M1 + M2).
func (r Result) Election() int { return r.Count[M1] + r.Count[M2] }

// Run builds the ring for the given ids (messages go P0 -> P1 -> ... -> P(n-1) -> P0),
// starts one goroutine per process and lets them take steps in a random order.
func Run(algo Algorithm, ids []int, rng *rand.Rand) Result {
	n := len(ids)
	links := make([]chan Message, n) // links[i] carries messages from P(i-1) to P(i)
	for i := range links {
		links[i] = make(chan Message, 1)
	}
	done := make(chan stepLog)
	nodes := make([]*node, n)
	var wg sync.WaitGroup
	for i := range nodes {
		nodes[i] = &node{pos: i, n: n, state: State{ID: ids[i]},
			in: links[i], out: links[(i+1)%n], turn: make(chan turn), done: done}
		wg.Add(1)
		go nodes[i].run(algo, &wg)
	}

	res := Result{Algo: algo.Name, IDs: ids, Count: map[Kind]int{}, M2PerPhase: map[int]int{}}
	started := make([]bool, n)  // has the process woken up yet?
	pending := make([]int, n)   // messages sent to the process and not handled yet
	lastPhase := make([]int, n) // phase of the last M1 the process handled
	ready := newReadySet(n)     // processes that can take a step: not woken up yet or a message waiting
	for i := 0; i < n; i++ {
		ready.add(i)
	}
	for ready.size() > 0 { // when nothing is in flight any more, the run is over
		i := ready.random(rng)
		time.Sleep(Delay)
		res.Steps++
		nodes[i].turn <- turn{step: res.Steps, wake: !started[i]}
		r := <-done

		if r.wake {
			started[i] = true
		} else {
			pending[i]--
		}
		if pending[i] == 0 {
			ready.remove(i)
		}
		if next := (i + 1) % n; len(r.sent) > 0 {
			pending[next] += len(r.sent)
			ready.add(next)
		}
		res.record(r, lastPhase)
	}
	for _, v := range nodes {
		close(v.turn)
	}
	wg.Wait()
	for _, v := range nodes {
		res.Final = append(res.Final, v.state)
	}
	return res
}

// startsPhase tells whether a step made the process active in a new phase.
func startsPhase(before, after State) bool {
	return after.Status == Active && (before.Status != Active || after.Phase != before.Phase)
}

// record updates the statistics after one step.
func (res *Result) record(r stepLog, lastPhase []int) {
	if r.handled.Kind == M1 {
		lastPhase[r.pos] = r.handled.Phase
	}
	if r.wake || startsPhase(r.before, r.after) {
		res.Activations = append(res.Activations, Activation{r.pos, r.after.Phase, r.after.Max})
		if r.after.Phase+1 > res.Phases {
			res.Phases = r.after.Phase + 1
		}
	}
	for _, m := range r.sent {
		res.Count[m.Kind]++
		if m.Kind == M2 {
			phase := lastPhase[r.pos]
			if r.before.Status == Active {
				phase = r.before.Phase
			}
			res.M2PerPhase[phase]++
		}
	}
}

// Check verifies the outcome: exactly one leader, every process knows it,
// and the announced leader is the maximum id.
func (res Result) Check() error {
	maxID := 0
	for _, id := range res.IDs {
		if id > maxID {
			maxID = id
		}
	}
	leaders := 0
	for i, s := range res.Final {
		if s.Status == Leader {
			leaders++
		}
		if !s.Done || s.KnownLeader != maxID {
			return fmt.Errorf("P%d does not know the right leader (knows %d, max is %d)", i, s.KnownLeader, maxID)
		}
	}
	if leaders != 1 {
		return fmt.Errorf("%d processes think they are the leader", leaders)
	}
	if res.Algo == "B" && res.IDs[res.LeaderPos()] != maxID {
		return fmt.Errorf("the leader P%d does not own the maximum id", res.LeaderPos())
	}
	return nil
}

// LeaderPos is the position of the process in state Leader (-1 if none).
func (res Result) LeaderPos() int {
	for i, s := range res.Final {
		if s.Status == Leader {
			return i
		}
	}
	return -1
}

// readySet is the set of processes that may take the next step; random() picks one uniformly.
type readySet struct {
	items []int
	where []int // index in items, or -1
}

func newReadySet(n int) *readySet {
	r := &readySet{where: make([]int, n)}
	for i := range r.where {
		r.where[i] = -1
	}
	return r
}

func (r *readySet) size() int { return len(r.items) }

func (r *readySet) random(rng *rand.Rand) int { return r.items[rng.Intn(len(r.items))] }

func (r *readySet) add(i int) {
	if r.where[i] < 0 {
		r.where[i] = len(r.items)
		r.items = append(r.items, i)
	}
}

func (r *readySet) remove(i int) {
	k := r.where[i]
	if k < 0 {
		return
	}
	last := r.items[len(r.items)-1]
	r.items[k], r.where[last] = last, k
	r.items = r.items[:len(r.items)-1]
	r.where[i] = -1
}
