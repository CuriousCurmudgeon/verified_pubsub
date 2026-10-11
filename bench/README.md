# Benchmarks

Both compare VerifiedPubSub against the same work done with plain `Phoenix.PubSub`.

    mix run bench/runtime.exs
    elixir bench/compile.exs <out_dir>

`runtime.exs` measures broadcasts, a subscriber round trip, fan-out and message size.
`compile.exs` generates synthetic projects at several manifest sizes, times a clean
compile of each, and times an incremental compile after editing the manifest. It takes
10-15 minutes. Spark stores each entity's source path in the manifest, so pick an
`out_dir` about as long as a real project's path, or the manifest numbers come out high.

## Results

Apple M2 Max, Elixir 1.20.4 / OTP 29, 2026-10-11.

### Runtime

Spark 2.7.2 and 2.7.6 gave the same numbers, within noise: Spark is not on the broadcast
path. Fan-out varies by about ±10% between runs.

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

Spark 2.7.6:

| Topics | Plain  | Verified | Manifest alone | Manifest .beam | Files recompiled by a manifest edit |
| ------ | ------ | -------- | -------------- | -------------- | ----------------------------------- |
| 20     | 0.11 s | 0.37 s   | 0.26 s         | 273 KB         | 71 (0.37 s)                         |
| 100    | 0.28 s | 4.5 s    | 4.1 s          | 1.3 MB         | 301 (4.4 s)                         |
| 250    | 0.63 s | 24.2 s   | 23.3 s         | 3.3 MB         | 751 (24.2 s)                        |

Spark 2.7.2:

| Topics | Plain  | Verified | Manifest alone | Manifest .beam | Files recompiled by a manifest edit |
| ------ | ------ | -------- | -------------- | -------------- | ----------------------------------- |
| 20     | 0.11 s | 1.6 s    | 1.5 s          | 1.5 MB         | 71 (1.0 s)                          |
| 100    | 0.32 s | 8.7 s    | 8.9 s          | 7.4 MB         | 301 (8.7 s)                         |
| 250    | 0.63 s | 28.9 s   | 27.9 s         | 18 MB          | 751 (28.9 s)                        |

Nearly all of the cost is the manifest. Every caller and subscriber depends on it at
compile time, so any edit to it, even a description, recompiles all of them.

Spark 2.7.2 compiled the whole DSL as a literal three times, once in `entities/1` and
twice in `__verify_spark_dsl__/1`. 2.7.4 stopped that, which shrank the manifest .beam
about sixfold. What remains is the manifest's module body: Elixir compiles it into a
temporary module before running it, and the Erlang compiler's variable analysis
(`core_lib:vu_expr`) is quadratic in its size. With 100 topics that compile takes 1.2 s,
against 70 ms for the manifest module itself; with 200 topics, 4.9 s against 140 ms.
