Documentation
=============

This guide says where each kind of text lives and how much of it to write. It covers a doc attribute, a code comment,
and a test name.

The rule format and the `PROSE` rules of [STYLE.md](STYLE.md#rule-format) apply here too.

## Where text lives

Each fact has one home. Link to that home. Do not repeat the fact.

| Text | Home |
|---|---|
| What a module does | The `@moduledoc`. See [`DOC-2`](#doc-2). |
| A tradeoff that serves time or space | `## Tradeoffs` in the `@moduledoc`. See [`DOC-5`](#doc-5). |
| How a mechanism works | A code comment beside the code. See [`DOC-3`](#doc-3). |
| The values of a type, the fields of a struct, or the keys of a returned map | The `@typedoc`. See [`DOC-7`](#doc-7). |
| What a function takes, returns, and refuses | The `@doc`. See [`DOC-2`](#doc-2). |
| The detail of the behavior | The tests. See [`SPEC-1`](#spec-1). |

## DOC - Docs and comments

A doc states the contract of a function or what a module does. A comment states how a mechanism works and why. A test
states the detail of the behavior. No text describes code that it does not sit on.

### DOC-1

When a module or a function is public, document it:

- A module takes a `@moduledoc`, unless it is a test module.
- A function takes a `@doc` and a `@spec`, unless it implements a behaviour.
- A macro takes a `@doc` and no `@spec`.[^macro]

### DOC-2

When you write a doc, open it with a one-line summary. Add at most three sentences to a `@doc` and five to a
`@moduledoc`. A section does not count toward the limit. A `@doc` gives the cases that a caller must handle, and only
the meaning that the `@spec` cannot carry.[^spec] A `@moduledoc` summarizes what the module does.

### DOC-3

When you write a code comment, explain a mechanism that a reader cannot see in the code, and say why. When a source
outside the code exists, such as an issue, an upstream bug, or a specification, cite it. Do not explain what the code
does.

A `TODO`, a `FIXME`, or a `HACK` comment needs no source. These comments are for a person, so write one only when the
user asks.

### DOC-4

When you write a doc or a comment, describe only the code that it sits on. Do not name a caller or a module that reads
its output. Do not describe what happens elsewhere at runtime. Name another module only to link to it. Leave out a
section that only points to another module.[^stale]

### DOC-5

When a doc needs a section, use one of these second level headers:

- `## Options` in a `@doc`: each option and its default. When options pass to another function, name that function,
  and list only the options that this one adds.
- `## Tradeoffs` in a `@moduledoc`: each choice that is not obvious and that serves time or space. Say what the module
  gives up, then what it gains.
- `## Message Passing` in the `@moduledoc` of a module that sends or receives a message. See [`DOC-6`](#doc-6).
- `## Usage` in a doc: how a caller uses the function or the module.
- `## Examples` in a doc: each example call and its result.

### DOC-6

When you write a `## Message Passing` list, put the PubSub bullets first and the direct messages next. Give one bullet
to each topic and to each direct message.

- A PubSub bullet starts with `Publishes to` or `Subscribes to`. It names the topic, then its events, as in
  `Subscribes to "job:<job_id>" for "start_job" and "finish_job".`
- A direct bullet starts with `Sends` or `Receives`. It names the message as a term, as in
  ``Sends `{:refresh_items, items}` to the caller.``

Write a topic and an event as literal strings in quotes, never as the function that builds them. Mark the part of a
topic that varies, as in `"user:<user_id>"`. A direct message comes from `send/2` or `Process.send_after/3`. Leave out
a message that the same module sends and receives, as a timer.

### DOC-7

When a type has a fixed set of values, list them in its `@typedoc`. List the fields of a struct in the `@typedoc` of
`t:t/0`. When a function returns a map, give the map a `@type` and a `@typedoc`, and list its keys in the `@typedoc`.
When a function returns an enumerated value, give it a `@type` and a `@typedoc`. Give each item one line.

### DOC-8

When you format a doc, follow [Writing documentation](https://hexdocs.pm/elixir/writing-documentation.html) of the
Elixir guides.

## SPEC - Test names

A test name reads as a requirement. It names the contract of a function, a macro, or a construct that the user
writes, or the outcome of a state.

### SPEC-1

When a test covers a function or a macro, name the `describe` after it and its arity, as in `topic/2`. When a
`describe` needs its own setup, add a qualifier after the arity that names its data, as in
`topic/2 with a param-free topic`. When the tests cover the same contract on many functions or macros, list them, as
in `broadcast_from/5, broadcast_from!/5`. Name a mix task after its command, as `mix verified_pubsub.install`. State
the contract in the `test` name.

### SPEC-2

When a test checks whether source compiles, name the `describe` after the construct that the source holds:

- A macro follows [`SPEC-1`](#spec-1), as `broadcast!/4`.
- A DSL entity takes its name, as `topic`, `message`, or `field`.
- An option of a DSL entity takes the entity and the option, as `field :description`. When the tests cover the same
  option on many entities, list the entities, as `topic, message, field :description`.
- A `use` or an `import` takes that line, as `use VerifiedPubSub.Subscriber`.
- A check across many entities takes the `use` and a qualifier, as
  `use VerifiedPubSub.Manifest with topics of a similar shape`.

Do not name a transformer, a callback, or another function that the test does not call.[^construct] State in the
`test` name whether the source compiles. When it does not compile, name what the error holds, as in
`"a duplicate topic name is a compile error that names the first declaration"`.

### SPEC-3

When a test covers behavior that no one function or construct owns, name the `describe` after the state that its
tests share, as in `a message from another manifest`. State the outcome of that state in the `test` name. Leave out the
`describe` when the test module holds one state for every test.

## Footnotes

[^stale]: The code around the text is not visible from the text. The text becomes wrong, no check reports it, and then
it misleads a reader.

[^construct]: The user writes the construct, and the error points at it. A transformer or a callback is an internal
name. It can move to another module while the test still passes, and then the name is wrong.

[^macro]: A spec on a macro describes the quoted form that the macro receives, not the value that the caller writes.
A reader takes it for the second.

[^spec]: The spec holds the shape of the value, and the compiler checks it. A sentence that repeats that shape can
become wrong, and no check reports it.
