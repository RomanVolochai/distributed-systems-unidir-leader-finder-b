package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	n := flag.Int("n", 8, "number of processes (used when no -ids/-file is given)")
	layout := flag.String("layout", "random", "ids for -n: random | asc | desc | bitrev (see makeIDs)")
	idsFlag := flag.String("ids", "", "comma-separated ids in ring order, e.g. 5,2,6,3,1,4,8,7")
	file := flag.String("file", "", "file with the ids in ring order (separated by spaces, commas or newlines)")
	algoName := flag.String("algo", "B", "algorithm: B (the paper's Algorithm B) or A (the simple one)")
	seed := flag.Int64("seed", 0, "random seed for the ids and the schedule (0 = take it from the clock)")
	flag.DurationVar(&Delay, "delay", 0, "pause before every step, e.g. 200ms")
	quiet := flag.Bool("quiet", false, "do not print every message, only the summary")
	bench := flag.Bool("bench", false, "print the table of message counts for n = 8, 16, ..., -maxn")
	maxN := flag.Int("maxn", 4096, "largest n for -bench")
	trials := flag.Int("trials", 20, "random rings per n for -bench")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano() % 1_000_000
	}
	rng := rand.New(rand.NewSource(*seed))

	if *bench {
		Verbose = false
		runBench(*maxN, *trials, rng)
		return
	}

	algo, ok := algorithms[strings.ToUpper(*algoName)]
	if !ok {
		fail("unknown algorithm %q, use A or B", *algoName)
	}
	ids, err := readIDs(*idsFlag, *file, *n, *layout, rng)
	if err != nil {
		fail("%v", err)
	}
	Verbose = !*quiet

	fmt.Printf("Algorithm %s on a ring of %d processes, seed %d\n", algo.Name, len(ids), *seed)
	fmt.Printf("Messages go P0 -> P1 -> ... -> P%d -> P0\n", len(ids)-1)
	if len(ids) <= 64 {
		fmt.Printf("ids: %v\n\n", ids)
	}
	res := Run(algo, ids, rng)
	printSummary(res)
	if err := res.Check(); err != nil {
		fail("WRONG RESULT: %v", err)
	}
}

func printSummary(res Result) {
	n := float64(len(res.IDs))
	bound := 1.5*n*math.Log2(n) + 8.0/3.0*n
	leader := res.LeaderPos()
	fmt.Printf("\n================ summary (algorithm %s) ================\n", res.Algo)
	fmt.Printf("n                         %d\n", len(res.IDs))
	fmt.Printf("leader                    P%d, which holds the maximum %d\n", leader, res.Final[leader].KnownLeader)
	fmt.Printf("phases                    %d\n", res.Phases)
	fmt.Printf("M1 messages               %d\n", res.Count[M1])
	fmt.Printf("M2 messages               %d\n", res.Count[M2])
	fmt.Printf("election total (M1 + M2)  %d\n", res.Election())
	if n > 1 {
		fmt.Printf("  per n*log2(n)           %.3f\n", float64(res.Election())/(n*math.Log2(n)))
		if res.Algo == "B" {
			fmt.Printf("  bound 1.5 n log n + 8/3 n = %.1f\n", bound)
		} else {
			fmt.Printf("  bound 2 n log n + n     = %.1f\n", 2*n*math.Log2(n)+n)
		}
	}
	fmt.Printf("leader announcement       %d\n", res.Count[LeaderMsg])
	fmt.Printf("all messages              %d\n", res.Election()+res.Count[LeaderMsg])
	fmt.Printf("scheduler steps           %d\n", res.Steps)
}

// runBench prints the table used in the report.
func runBench(maxN, trials int, rng *rand.Rand) {
	fmt.Println("Messages of the election (M1 + M2), without the final announcement.")
	fmt.Println("random = average over", trials, "random rings; asc/desc = ids sorted along the ring;")
	fmt.Println("bitrev = bit-reversal order, which needs the most phases.")
	fmt.Println()
	fmt.Printf("%6s %8s %7s %10s %10s %10s %9s %10s %9s %12s\n",
		"n", "layout", "phases", "M1", "M2", "B total", "B/nlogn", "A total", "A/nlogn", "B bound")
	for n := 8; n <= maxN; n *= 2 {
		nlogn := float64(n) * math.Log2(float64(n))
		bound := 1.5*nlogn + 8.0/3.0*float64(n)
		for _, layout := range []string{"random", "asc", "desc", "bitrev"} {
			t := trials
			if layout != "random" {
				t = 1
			}
			var phases, m1, m2, a float64
			for k := 0; k < t; k++ {
				ids := makeIDs(n, layout, rng)
				rb := Run(algorithms["B"], ids, rng)
				ra := Run(algorithms["A"], ids, rng)
				for _, r := range []Result{rb, ra} {
					if err := r.Check(); err != nil {
						fail("n=%d %s: %v", n, layout, err)
					}
				}
				phases += float64(rb.Phases)
				m1 += float64(rb.Count[M1])
				m2 += float64(rb.Count[M2])
				a += float64(ra.Election())
			}
			ft := float64(t)
			fmt.Printf("%6d %8s %7.1f %10.0f %10.0f %10.0f %9.3f %10.0f %9.3f %12.0f\n",
				n, layout, phases/ft, m1/ft, m2/ft, (m1+m2)/ft, (m1+m2)/ft/nlogn, a/ft, a/ft/nlogn, bound)
		}
	}
}

// readIDs builds the list of ids from -ids, -file or -n/-layout.
func readIDs(idsFlag, file string, n int, layout string, rng *rand.Rand) ([]int, error) {
	text := idsFlag
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		text = string(data)
	}
	if text == "" {
		if n < 1 {
			return nil, fmt.Errorf("-n must be at least 1")
		}
		if layout != "random" && layout != "asc" && layout != "desc" && layout != "bitrev" {
			return nil, fmt.Errorf("unknown layout %q", layout)
		}
		return makeIDs(n, layout, rng), nil
	}
	var ids []int
	seen := map[int]bool{}
	sc := bufio.NewScanner(strings.NewReader(strings.ReplaceAll(text, ",", " ")))
	sc.Split(bufio.ScanWords)
	for sc.Scan() {
		id, err := strconv.Atoi(sc.Text())
		if err != nil || id < 1 {
			return nil, fmt.Errorf("bad id %q: ids must be positive integers", sc.Text())
		}
		if seen[id] {
			return nil, fmt.Errorf("id %d appears twice: ids must be distinct", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no ids given")
	}
	return ids, nil
}

// makeIDs returns the ids 1..n in ring order:
// random   a random permutation,
// asc      1, 2, ..., n (every process gets a smaller value from the left),
// desc     n, ..., 2, 1 (every process gets a bigger value from the left),
// bitrev   the bit-reversal permutation: local maxima are spread evenly, so
//
//	only half of the processes survive each phase (about log n phases).
func makeIDs(n int, layout string, rng *rand.Rand) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i + 1
	}
	switch layout {
	case "random":
		rng.Shuffle(n, func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	case "desc":
		for i := range ids {
			ids[i] = n - i
		}
	case "bitrev":
		bits := 0
		for 1<<bits < n {
			bits++
		}
		key := func(i int) int { // i with its lowest `bits` bits reversed
			r := 0
			for b := 0; b < bits; b++ {
				r = r<<1 | (i>>b)&1
			}
			return r
		}
		sort.Slice(ids, func(a, b int) bool { return key(ids[a]-1) < key(ids[b]-1) })
	}
	return ids
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
