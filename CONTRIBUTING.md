# Contributing

Thank you for your interest in VerifiedPubSub.

## Open an issue first

Open an issue before you send a pull request, for a bug fix and for a feature.
VerifiedPubSub is small on purpose, and some features that seem reasonable work against
its purpose. When we agree on the change first, you do not write a pull request that
does not get merged.

A bug report is most useful when it gives these items:

- the smallest manifest and call site that show the bug
- the error that you got
- your Elixir version
- your OTP version

## Setup

`.tool-versions` holds the Elixir and Erlang versions of the project. With
[asdf](https://asdf-vm.com) or [mise](https://mise.jdx.dev) installed:

```sh
asdf install   # or: mise install
mix deps.get
mix test
```

CI also tests against the oldest versions that `mix.exs` supports, Elixir 1.16 on OTP 26.

## Before you send a pull request

CI runs these commands, so run them locally first:

```sh
mix format --check-formatted
mix deps.unlock --check-unused
mix compile --warnings-as-errors
mix test
```

Much of the behavior of the library is a compile error. So many tests compile a module
from a source string and assert on the error that it raises. The helpers for this are in
`test/support/compile_helper.ex`. Tests compile with `debug_info: true`, which `mix.exs`
sets. Spark records where a DSL entity is declared only when debug info is on, and many
tests assert that an error points at the declaration.

When you change what the library accepts or how it behaves, update the README,
`usage-rules.md` and `CHANGELOG.md` in the same pull request.

## Benchmarks

`bench/` compares VerifiedPubSub with plain `Phoenix.PubSub`. `bench/README.md` tells how
to run it. When a change touches the broadcast path or the compilation of the manifest,
say how the numbers changed.
