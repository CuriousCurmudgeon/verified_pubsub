# Benchmarks

Both compare VerifiedPubSub against the same work done with plain `Phoenix.PubSub`.

    mix run bench/runtime.exs
    elixir bench/compile.exs <out_dir>

`runtime.exs` measures broadcasts, a subscriber round trip, fan-out and message size.
`compile.exs` generates synthetic projects at several manifest sizes, times a clean
compile of each, and times an incremental compile after editing the manifest. It takes
10-15 minutes. Spark stores each entity's source path in the manifest, so pick an
`out_dir` about as long as a real project's path, or the manifest numbers come out high.

## Baseline

Apple M2 Max, Elixir 1.20.4 / OTP 29, Spark 2.7.2, 2026-10-11.

### Runtime

| Scenario                                      | Plain   | Verified |
| --------------------------------------------- | ------- | -------- |
| broadcast, no subscribers, 2 fields           | 0.56 µs | 1.08 µs  |
| broadcast, no subscribers, 20 fields          | 0.55 µs | 2.49 µs  |
| round trip: broadcast, receive, handle_info/2 | 0.65 µs | 1.27 µs  |
| broadcast to 100 subscribers                  | 96 µs   | 110 µs   |
| `{:list, :string}` field, 1,000 elements      | 0.57 µs | 7.1 µs   |
| `{:list, :string}` field, 100,000 elements    | 0.54 µs | 614 µs   |

A message is 39 heap words and 182 bytes in external term format, against 17 and 46 for
a plain `{event, payload}` tuple.

### Compile

Each topic has 5 events of 5 fields. Callers and subscribers scale with the topic count.
The `out_dir` path was about 150 characters, which makes the manifest slower than in a
typical project.

| Topics | Plain  | Verified | Manifest alone | Manifest .beam | Files recompiled by a manifest edit |
| ------ | ------ | -------- | -------------- | -------------- | ----------------------------------- |
| 20     | 0.11 s | 1.6 s    | 1.5 s          | 1.5 MB         | 71 (1.0 s)                          |
| 100    | 0.32 s | 8.7 s    | 8.9 s          | 7.4 MB         | 301 (8.7 s)                         |
| 250    | 0.63 s | 28.9 s   | 27.9 s         | 18 MB          | 751 (28.9 s)                        |

Nearly all of the cost is the manifest module. Spark compiles the whole DSL as a literal
three times, once in `entities/1` and twice in `__verify_spark_dsl__/1`. Every caller and
subscriber depends on the manifest at compile time, so any edit to it, even a
description, recompiles all of them.
