# Contributing

Thanks for your interest in VerifiedPubSub.

## Open an issue first

Please open an issue before you send a pull request, for a bug fix as well as a feature.
VerifiedPubSub is small on purpose, and some reasonable-sounding features work against
what it is for. Agreeing on the change first saves you from writing a pull request that
does not get merged.

A bug report is most useful with the smallest manifest and call site that reproduce it,
the error you got, and your Elixir and OTP versions.

## Setup

The Elixir and Erlang versions the project develops against are in `.tool-versions`. With
[asdf](https://asdf-vm.com) or [mise](https://mise.jdx.dev) installed:

```sh
asdf install   # or: mise install
mix deps.get
mix test
```

CI also tests against the oldest versions `mix.exs` supports, Elixir 1.16 on OTP 26.

## Before you send a pull request

CI runs these, so run them locally first:

```sh
mix format --check-formatted
mix deps.unlock --check-unused
mix compile --warnings-as-errors
mix test
```

Much of the library's behavior is a compile error, so many tests compile a module from a
source string and assert on the error it raises, using the helpers in
`test/support/compile_helper.ex`. Tests compile with `debug_info: true`, set in `mix.exs`:
Spark only records where a DSL entity is declared when debug info is on, and many tests
assert that an error points at the declaration.

If you change what the library accepts or how it behaves, update the README,
`usage-rules.md` and `CHANGELOG.md` in the same pull request.

## Benchmarks

`bench/` compares VerifiedPubSub with plain `Phoenix.PubSub`. See `bench/README.md` for
how to run it. A change to the broadcast path or to manifest compilation should say how
the numbers moved.
