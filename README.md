# Cadence

Reference implementation of **Cadence**, a compact sketch for *Sustained Stable
Item Lookup* (SSIL) in high-speed data streams.

Time is cut into fixed epochs. Let `x_t(u)` be the quantity item `u` delivers in
epoch `t`. Item `u` is a **Sustained Stable Item** (SSI) at epoch `t` when all
three conditions hold over the `P` most recent epochs:

| condition | meaning | test |
|---|---|---|
| continuity | active in every one of the last `P` epochs | `x_{t-i}(u) > 0`, `i = 0..P-1` |
| significance | large enough to matter | `mu_t(u) >= mu_min` |
| stability | low temporal fluctuation | `MAD_t(u) <= tau` |

Cadence answers this online with one table of `m x d` buckets. Each bucket holds
one candidate item and the three descriptors that decide the three gates: a
continuity streak `S`, an exponentially weighted mean `mu`, and an exponentially
weighted mean absolute deviation `mad`. There is no per-item history and no
second table, so an update is `O(1)` and the space is `Theta(m*d)`.

This repository contains the mechanism and nothing else: no baselines and no
comparison harness.

## Repository layout

```
src/cadence.h      the sketch          <- this is the algorithm
src/murmurhash.h   MurmurHash2-64B (public domain), the only dependency
src/trace.h        binary reader for the datasets used in the paper
src/main.cpp       demo driver: exact SSI oracle, accuracy, durability, throughput
Makefile
```

`src/cadence.h` is header-only and self-contained apart from `murmurhash.h`.
Everything in `src/main.cpp` is demo and evaluation scaffolding, not part of the
sketch.

## Build and run

```bash
make            # builds ./cadence with 8-byte keys
make run        # runs the built-in synthetic stream, no dataset needed
```

Requires a C++11 compiler and `make`. Build for another key width with
`make KEYLEN=4` (zipf, webdocs, campus source IP) or `make KEYLEN=13` (campus
5-tuple).

Example output, 40 KB of memory over a 2-million-update synthetic stream of 200
epochs carrying 120 stable and 20 bursty items:

```
memory     40.0 KB -> 2 rows x 568 buckets
parameters P=12  mu_min=8  tau=2.23607  alpha=0.1  lambda=16  L=2  key=8 B

ground truth   26005 SSI verdicts
reported       25529 verdicts, 24411 correct
recall         0.9387
precision      0.9562
F1             0.9474
rate ARE       0.0114  (over 24411 matched verdicts)

durable SSIs   578 runs of length >= 2, longest 189 epochs
throughput     18.4 Mops  (best of 3 passes, sweep included)
```

To run on a trace instead:

```bash
./cadence --trace data/caida.dat --format caida --mem 40 --epoch 10000
```

On a 31.3-million-packet CAIDA trace with 40 KB of memory and 10,000-update
epochs, that reports:

```
ground truth   31303 SSI verdicts
reported       30620 verdicts, 28168 correct
recall         0.8998
precision      0.9199
F1             0.9098
rate ARE       0.0156
durable SSIs   1112 runs of length >= 2, longest 3115 epochs
```

Throughput figures depend on the machine and on the epoch length, since each
epoch boundary sweeps the table.

The traces are not included. Each file is a flat binary stream of fixed-size
records; `src/trace.h` holds the key offset and width for `caida`, `caida_src`,
`campus`, `campus_src`, `zipf` and `webdocs`.

`./cadence --help` lists every option.

## Using the sketch

```cpp
#include "cadence.h"

cadence::Config cfg;                     // paper defaults, see below
cadence::Cadence<8> sk(2, d, cfg);       // 2 rows x d buckets, 8-byte keys

for (each arrival) {
    if (epoch changed) sk.NewEpoch(t);   // mandatory, once per epoch
    bool isSSI = sk.Update(key);         // O(1)
}

std::vector<cadence::Report> ssi;
sk.Query(t, ssi);                        // every current SSI, with descriptors
```

Keys are compared and hashed over exactly `KEYLEN` bytes, so pad shorter keys
with zeros.

**The per-epoch sweep is mandatory, not an optimisation.** A bucket that receives
no update during an epoch is brought up to date only by the sweep, and the
continuity streak would otherwise be wrong. Call `NewEpoch(t)` exactly once per
epoch boundary; `Query(t, ...)` subsumes it. The sweep's cost is inside the
throughput the demo reports.

### Weighted streams (Cadence-W)

Pass a weight to `Update`:

```cpp
sk.Update(key, bytes);     // x_t(u) becomes the total weight u delivers in epoch t
```

The accumulator adds `w` instead of `1` and a fresh bucket is initialised at `w`.
Nothing else changes.

### Durability

Two extra words per bucket give the maximal intervals over which an item stays an
SSI, with no auxiliary structure:

```cpp
sk.Flush();                                    // end of stream: close open runs
const std::vector<cadence::Interval>& runs = sk.DurableRuns();   // [start, end]

std::vector<cadence::Interval> open;
sk.QueryDurable(t, open);                      // runs still in progress at epoch t
```

Only runs of at least `Config::L` epochs are reported.

## Parameters

| field | default | meaning |
|---|---|---|
| `P` | 12 | epochs of consecutive activity required |
| `muMin` | 8 | significance threshold |
| `tau` | sqrt(5) | stability threshold on the mean absolute deviation |
| `alpha` | 0.1 | EWMA smoothing factor |
| `lambda` | 16 | sharpness of the replacement decay |
| `L` | 2 | shortest durable run reported |
| `hashSeed` | 0x100 | base seed of the row hash family |

`P`, `muMin` and `tau` define the task; `alpha` and `lambda` are the sketch's own
knobs. Two rows (`m = 2`) is the setting used throughout the paper.

## How an update works

1. Probe `m` buckets, one per row.
2. Any probed bucket still tagged with an older epoch is rolled over first: its
   completed epoch is committed into `S`, `mu` and `mad`.
3. On a **match**, add the weight to the in-progress epoch counter and stop.
4. On an **empty** bucket, insert there and stop. A fresh bucket starts with
   `S = 0` and `mu = mu_min`, so a newly inserted item is not penalised in the
   replacement score before it has any history.
5. Otherwise pick the victim with the smallest retention score

   ```
   score = S/P + mu/mu_min - mad/tau
   ```

   skipping any bucket that currently satisfies all three gates, which is never
   evicted. Replace the victim outright when its score is non-positive, and
   otherwise with probability `2^(-lambda * score)`. If every probed bucket is
   protected the update is discarded.

`UpdateMeanMAD` updates the mean first and then takes the deviation against the
just-updated mean:

```
mu  <- (1 - alpha) * mu  + alpha * x
mad <- (1 - alpha) * mad + alpha * |x - mu|
```
