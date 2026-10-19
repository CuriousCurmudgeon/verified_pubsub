# Benchmarks

Both benchmarks compare VerifiedPubSub with the same work in plain `Phoenix.PubSub`.

    mix run bench/runtime.exs
    elixir bench/compile.exs <out_dir>

`runtime.exs` measures broadcasts, a subscriber round trip, fan-out and message size.

`compile.exs` generates synthetic projects at several manifest sizes. It times a clean
compile of each project, and an incremental compile after an edit to the manifest. It
takes 10-15 minutes. Spark stores the source path of each entity in the manifest, so the
length of `out_dir` changes the results. Pick an `out_dir` about as long as the path of a
real project, or the manifest numbers are too high.

## Results

Apple M2 Max, Elixir 1.20.4 / OTP 29, 2026-10-11.

### Runtime

Spark 2.7.2 and 2.7.6 gave the same numbers, within noise, because Spark is not on the
broadcast path. Fan-out changes by about ±10% between runs.

| Scenario                                      | Plain   | Verified |
| --------------------------------------------- | ------- | -------- |
| broadcast, no subscribers, 2 fields           | 0.56 µs | 1.08 µs  |
| broadcast, no subscribers, 20 fields          | 0.55 µs | 2.49 µs  |
| round trip: broadcast, receive, handle_info/2 | 0.65 µs | 1.27 µs  |
| broadcast to 100 subscribers                  | 96 µs   | 110 µs   |
| `{:list, :string}` field, 1,000 elements      | 0.57 µs | 7.1 µs   |
| `{:list, :string}` field, 100,000 elements    | 0.54 µs | 614 µs   |

A message is 39 heap words and 182 bytes in external term format. A plain
`{event, payload}` tuple is 17 heap words and 46 bytes.

### Compile

Each topic has 5 events of 5 fields. The number of callers and subscribers grows with the
number of topics. The `out_dir` path was about 150 characters, so the manifest compiles
more slowly than in a typical project.

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

The manifest causes almost all of the cost. Every caller and subscriber depends on it at
compile time. So any edit to the manifest, even to a description, compiles all of them
again.

Spark 2.7.2 compiled the whole DSL as a literal three times: once in `entities/1`, and
twice in `__verify_spark_dsl__/1`. Spark 2.7.4 removed that, and the manifest .beam became
about six times smaller. The remaining cost is the module body of the manifest. Elixir
compiles the body into a temporary module before it runs it. The variable analysis of the
Erlang compiler (`core_lib:vu_expr`) is quadratic in the size of that module. With 100
topics, that compile takes 1.2 s, and the manifest module itself takes 70 ms. With 200
topics, the times are 4.9 s and 140 ms.
