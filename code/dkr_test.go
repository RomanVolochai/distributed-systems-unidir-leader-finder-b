package main

import (
	"math"
	"math/rand"
	"testing"
)

// Run all tests with:  go test
// They check correctness and the statements of Section 3.3 on many rings.

func init() { Verbose = false }

func TestElectsMaximum(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, algo := range []string{"A", "B"} {
		for n := 1; n <= 70; n++ {
			for _, layout := range []string{"random", "asc", "desc", "bitrev"} {
				ids := makeIDs(n, layout, rng)
				if err := Run(algorithms[algo], ids, rng).Check(); err != nil {
					t.Fatalf("algorithm %s, n=%d, ids %v: %v", algo, n, ids, err)
				}
			}
		}
	}
}

// Theorem 3.3.10: fewer than 1.5 n log n + 8/3 n messages.
func TestMessageBound(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for n := 2; n <= 2048; n = n*3/2 + 1 {
		for _, layout := range []string{"random", "asc", "desc", "bitrev"} {
			res := Run(algorithms["B"], makeIDs(n, layout, rng), rng)
			N := float64(n)
			if bound := 1.5*N*math.Log2(N) + 8.0/3.0*N; float64(res.Election()) >= bound {
				t.Fatalf("n=%d %s: %d messages, bound %.1f", n, layout, res.Election(), bound)
			}
		}
	}
}

// Lemma 3.3.6 (a), (c), Corollary 3.3.7 and Corollary 3.3.9.
func TestLemmas(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for trial := 0; trial < 50; trial++ {
		n := 2 + rng.Intn(300)
		ids := makeIDs(n, "random", rng)
		res := Run(algorithms["B"], ids, rng)
		origin := map[int]int{}
		for pos, id := range ids {
			origin[id] = pos
		}
		byPhase := map[int][]Activation{}
		for _, a := range res.Activations {
			byPhase[a.Phase] = append(byPhase[a.Phase], a)
		}
		for p, act := range byPhase {
			if float64(len(act)) > float64(n)/math.Pow(2, float64(p)) {
				t.Fatalf("|A(%d)| = %d > n/2^p", p, len(act))
			}
			for _, a := range act {
				if d := (a.Pos - origin[a.Value] + n) % n; d != 1<<p-1 {
					t.Fatalf("phase %d: value %d at distance %d from its origin, want 2^p-1", p, a.Value, d)
				}
				for _, b := range act {
					if d := (a.Pos - b.Pos + n) % n; d != 0 && d < 1<<p {
						t.Fatalf("phase %d: two active processes at distance %d < 2^p", p, d)
					}
				}
			}
		}
		for p, m2 := range res.M2PerPhase {
			if limit := float64(n) * math.Pow(2, float64(p)) / (math.Pow(2, float64(p+1)) - 1); float64(m2) > limit {
				t.Fatalf("phase %d: %d M2 messages > %.1f", p, m2, limit)
			}
		}
	}
}

// The ring from the slides: 26 election messages in 3 phases, leader P6.
func TestSlideExample(t *testing.T) {
	res := Run(algorithms["B"], []int{5, 2, 6, 3, 1, 4, 8, 7}, rand.New(rand.NewSource(4)))
	if res.Count[M1] != 21 || res.Count[M2] != 5 || res.Phases != 3 || res.LeaderPos() != 6 {
		t.Fatalf("got M1=%d M2=%d phases=%d leader=P%d", res.Count[M1], res.Count[M2], res.Phases, res.LeaderPos())
	}
}

// The order in which processes take steps changes the log, but not the messages:
// every process is deterministic and reads one FIFO link.
func TestScheduleDoesNotMatter(t *testing.T) {
	ids := makeIDs(200, "random", rand.New(rand.NewSource(5)))
	first := Run(algorithms["B"], ids, rand.New(rand.NewSource(6)))
	for s := int64(7); s < 12; s++ {
		res := Run(algorithms["B"], ids, rand.New(rand.NewSource(s)))
		if res.Count[M1] != first.Count[M1] || res.Count[M2] != first.Count[M2] {
			t.Fatalf("different counts for another schedule")
		}
	}
}
