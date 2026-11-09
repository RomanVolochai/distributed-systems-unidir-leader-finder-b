# Dolev–Klawe–Rodeh, Algorithm B: leader election on a unidirectional ring

D. Dolev, M. Klawe, M. Rodeh, *An O(n log n) Unidirectional Distributed Algorithm
for Extrema Finding in a Circle*, Journal of Algorithms 3 (1982), 245–260, Section 3.

| Folder | What is in it |
|---|---|
| `code/` | Go implementation (Algorithm B, plus Algorithm A for comparison) |
| `report/` | the report (`report.pdf`) and `bench.txt`, the output of `go run . -bench -seed 7` |
| `slides/` | the original presentation: `slides.pdf` |


## Running the code

**1. Install Go once** (version 1.21 or newer). On macOS: `brew install go`, or download
the installer from <https://go.dev/dl/>. Check with `go version`.

**2. Run it** from the `code` folder. No other packages are needed.

```sh
cd code
go run .                                    # random ring of 8 processes, full log
go run . -ids 5,2,6,3,1,4,8,7               # the ring used in the slides
go run . -file examples/ring8.txt -delay 300ms   # same ring, slowed down to follow live
go run . -n 1000 -quiet -seed 42            # big ring, summary only
go run . -n 1000 -quiet -seed 42 -algo A    # the same ring with Algorithm A
go run . -bench -seed 7                     # table of message counts for n = 8 ... 4096 (= report/bench.txt)
go test                                     # automatic checks (should print "ok")
```

| Flag | Meaning | Default |
|---|---|---|
| `-n` | number of processes (when no `-ids` / `-file` is given) | 8 |
| `-layout` | ids for `-n`: `random`, `asc`, `desc`, `bitrev` | random |
| `-ids` | ids in ring order, comma separated | — |
| `-file` | file with ids in ring order | — |
| `-algo` | `B` or `A` | B |
| `-seed` | seed for the random ids and the random order of steps; printed on every run, so a run can be repeated | from the clock |
| `-delay` | pause before every step, e.g. `200ms` | 0 |
| `-quiet` | print only the summary | off |
| `-bench`, `-maxn`, `-trials` | message-count table, largest n, random rings per n | off, 4096, 20 |

## What the output shows

Messages travel `P0 → P1 → … → P(n-1) → P0`. Every step is one process handling one event
(excerpt of `go run . -ids 5,2,6,3,1,4,8,7 -seed 1`):

```
step   17  P3 (id 3) got M2(value=6)                          -> ACTIVE max=6 phase=1
           phase 1 starts at P3 with value 6
           P3 sent M1(value=6, phase=1, counter=2) to P4
```

At the end comes a summary: the leader, the number of phases, M1 and M2 messages,
their total compared with the paper's bound 1.5 n log n + 8/3 n, and the n messages
of the final announcement.

## How the code is organised

| File | Contents |
|---|---|
| `algorithm_b.go` | **the algorithm**: `startB` and `handleB` (steps B1–B3), `passiveB` (B3) |
| `algorithm_a.go` | Algorithm A (`startA`, `handleA`), for comparison |
| `state.go`, `message.go` | the state of one process (`State`) and the messages (`M1`, `M2`, `LeaderMsg`) |
| `network.go` | one goroutine per process, links as channels with buffer 1, the random scheduler, `Delay`, logging, statistics |
| `main.go` | command-line flags, building the ring, the summary and the `-bench` table |
| `dkr_test.go` | tests: correct leader for n = 1…70, the message bound, Lemma 3.3.6 (a), (c) and Corollaries 3.3.7 and 3.3.9, the example ring from the slides, and that the order of steps does not change any count |

The handlers are pure functions `handle(state, message) -> (new state, messages to send)`.
A goroutine only loads its state, calls the handler and sends the result. The scheduler in
`Run` picks a random process that has something to do and lets it take exactly one step.
