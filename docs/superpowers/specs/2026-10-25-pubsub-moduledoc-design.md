# PubSub section in the moduledoc — Design

**Date:** 2026-10-25
**Status:** Approved for planning

## Purpose

Teams write a pub/sub section into the `@moduledoc` of a module by hand. The section says which topics and events the
module publishes and subscribes to. When the code changes, nothing updates that section, and the docs drift from the
code.

The macros of VerifiedPubSub already know each topic and event that a module uses, because they check each one at
compile time. With this change, the library writes the section, so the docs always agree with the code. The manifest
already writes its own `## Topics` section in the same way.

The section is for a person who reads the docs of a module and wants to know its messages. An example of the text that
teams write now:

```markdown
## Message Passing

  * Publishes to `"compliance:tcr_use_case:<account_id>"` for `:rate_limits_updated`.
  * Subscribes to `"compliance:tcr_use_case:<account_id>"` for `:submitted`,
    `:mms_sample_pending`, and `:exported`.
```

## Goals

1. A module that uses `VerifiedPubSub` or `VerifiedPubSub.Subscriber`, and that turns on docs, gets a `## PubSub`
   section at the end of its `@moduledoc`. See [Configuration](#configuration).
2. The section lists each topic that the module publishes to, with the events that it publishes.
3. The section lists each topic that the module subscribes to, with the events that it handles.
4. The code is the only source of the section. The user declares nothing a second time.

## Non-goals

- Per-function docs. Elixir evaluates `@doc` before the body of the function expands. Elixir also has no public API to
  change the `@doc` of a function after its `def`. The team that asked for this feature also moves from per-function
  docs to the `@moduledoc`.
- Calls across modules. When module A calls module B, and B broadcasts, the section of A does not show that broadcast.
  The section shows only the macro calls in the source of its own module.
- Descriptions of topics or events. The manifest docs hold them. The section can link to the manifest later.
- A section that the author places inside the `@moduledoc`, as `NimbleOptions.docs/2` allows. Elixir evaluates
  `@moduledoc` before the macro calls below it expand, so a call in the docstring cannot see them. A marker that the
  library replaces later works, but no known library does this, and a reader takes the call for an eager one. The user
  chose a section at the end.
- Application configuration. See [Configuration](#configuration).

## Anti-goals

- A section that disagrees with the code. Each bullet must come from a macro call that the module compiles.
- A change to the docs of an existing user who does not turn the section on.

## The section

The header is `## PubSub`. The team chose this name over its own `## Message Passing`, because the library names its
own concept.

```markdown
## PubSub

  * Publishes to `"groups:<group_id>"` for `:deleted`.
  * Publishes to `"owners:<owner_id>:pets"` for `:adopted` and `:rehomed`.
  * Subscribes to `"compliance:tcr_use_case:<account_id>"` for `:submitted`,
    `:exported`, and `:deactivated`.
  * Subscribes to `"system"`.
```

Rules:

- A bullet names the wire pattern of the topic, as `VerifiedPubSub.Topic.pattern/1` renders it. A param shows as
  `<param>`.
- A topic gets one `Publishes to` bullet. The bullet lists each event that the module publishes on that topic, once.
  Many call sites of one event give one entry.
- A topic gets one `Subscribes to` bullet. The bullet lists each event that a `handle_message` of the module handles.
- An event that `ignore_message` names does not appear. The user decided this.
- A topic where the module ignores each event gets no `Subscribes to` bullet. (My call. The module does nothing with
  the messages of that topic.)
- The `Publishes to` bullets come first, then the `Subscribes to` bullets. (My call.)
- Within each group, the bullets sort by wire pattern, and the events of a bullet sort by name. (My call. A sort keeps
  the text stable when call sites move.)
- An event list reads as English: `:a`, `:a` and `:b`, or `:a`, `:b`, and `:c`. (My call, from the example of the team.)
- A module with no bullet gets no section.

## Where the data comes from

### Publishes

`broadcast/3`, `broadcast/4`, `broadcast!/3`, `broadcast!/4`, `broadcast_from/4`, `broadcast_from/5`,
`broadcast_from!/4`, and `broadcast_from!/5` record `{topic, event}` in an accumulating module attribute when they
expand. They already resolve the topic and validate the event at that time.

A call inside a private function, an anonymous function, or any other place in the module counts. The section lists
the module, not each function, so the place of the call does not matter.

A call inside a `handle_message` body counts too. A subscriber that handles one event and broadcasts another is common.
That body does not expand where the user writes it. `VerifiedPubSub.Subscriber.__before_compile__/1` puts it into the
dispatch clauses that it returns, so a `broadcast!` in it expands only after each `@before_compile` hook runs. See
the timing rule in the next section.

### Subscribes

- In a module that uses `VerifiedPubSub.Subscriber`, the data comes from the `handle_message` clauses. The Subscriber
  already collects them in `@verified_pubsub_clauses` for its coverage check. A clause with a list of events adds each
  event.
- In a module that does not use `VerifiedPubSub.Subscriber`, `subscribe/1` and `subscribe/2` record the topic. The
  module has its own `handle_info/2`, so the library does not know which events it handles. The bullet has no `for`
  list. (My call.)
- In a module that uses `VerifiedPubSub.Subscriber`, a `subscribe` call does not add a bullet. The handlers are the
  source of truth there. (My call. Without this rule, a topic where the module ignores each event gets a bullet from
  its `subscribe` call.)

`unsubscribe` and `topic` record nothing.

## How the section gets into the moduledoc

When docs are on, the module reads the recorded data and renders the section. It adds the section to the end of the `@moduledoc` that
the user wrote, after a blank line.

The render must run after each macro call in the module expands, including the `handle_message` bodies. A render
inside a `@before_compile` hook runs too early for a Subscriber module. Instead, the code that a hook returns ends with
a call that renders the section. In a Subscriber module, that call comes after the dispatch clauses, so the bodies
expand first. In a module with both `use` lines, the code of the Subscriber hook makes the render, because the hook of
`use VerifiedPubSub` runs too early.

- `@moduledoc false` keeps the module hidden. The library adds no section.
- A module with no `@moduledoc` gets one that holds only the section. (My call. ExDoc lists the module either way.)
- `Module.put_attribute/3` for `:moduledoc` at `@before_compile` needs the `{line, doc}` form. Keep the line of the
  user's `@moduledoc` when one exists.
- A module can use both `VerifiedPubSub` and `VerifiedPubSub.Subscriber` with the same manifest. It must get one
  section, so the `@before_compile` hook must run its render once.

`VerifiedPubSub.Docs` renders the `## Topics` section of the manifest now. The render of this section goes there too.

## Configuration

The section is off by default, so an existing user sees no change. A `docs` option turns it on. The user chose this.

The manifest sets the default for each module that binds it:

```elixir
use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub, docs: true
```

The `use` line of one module overrides the manifest:

```elixir
use VerifiedPubSub, manifest: MyApp.Topics, docs: false
use VerifiedPubSub.Subscriber, manifest: MyApp.Topics, docs: true
```

Rules:

- `docs` is a boolean on `VerifiedPubSub.Manifest`, `VerifiedPubSub`, and `VerifiedPubSub.Subscriber`. The manifest
  default is `false`.
- The manifest takes the option through its Spark `opt_schema`. Its `handle_opts/1` defines `__verified_pubsub_docs__/0`,
  as it defines `__verified_pubsub_name__/0` now. The macros read it at expansion. The manifest compiles before each
  module that binds it, so the function exists then.
- A module with no `docs` on its `use` lines takes the value of its manifest.
- A module with both `use` lines takes one value. When only one line gives `docs`, that value applies. When the two
  lines give different values, the module does not compile, and the error names both values. (My call.)
- `@moduledoc false` still hides a module with docs on.

The library uses no application configuration. The
[library guidelines](https://hexdocs.pm/elixir/library-guidelines.html#avoid-application-configuration) of Elixir
advise against it, because the config of a dependency is global to the app. A macro also reads it when the caller
compiles, so a change to it takes effect only after a recompile. The manifest already holds the shared settings of its
modules, so it is the shared place for this one too.

## Tests

Follow the pattern of `test/verified_pubsub/docs_test.exs`. It compiles a module with docs on, writes the `.beam` to a
`tmp_dir`, and reads the moduledoc with `Code.fetch_docs/1`.

Cover at least:

- a broadcast of each arity group, and from a private function
- a broadcast inside a `handle_message` body
- many call sites of one event, which give one entry
- events of one topic in one bullet, sorted
- a `handle_message` with one event and with a list of events
- `ignore_message` events left out, and a topic with only ignored events left out
- a bare `subscribe` in a module without the Subscriber
- a module with both `use` lines, which gets one section
- `@moduledoc false`, no `@moduledoc`, and a user `@moduledoc` that comes first
- a module with no pub/sub calls, which gets no section
- docs off by default: a module with no `docs` option and a manifest with no `docs` option gets no section
- `docs: true` on the manifest, and `docs: false` on a `use` line that overrides it
- `docs: true` on a `use` line with a manifest that has docs off
- a module with both `use` lines that give different values, which does not compile

## Docs to update

Per `AGENTS.md`: `README.md`, `usage-rules.md`, and `CHANGELOG.md`. The change ships in v0.3.0, because v0.2.0 is on
Hex. Branch from `main`. The moduledocs of `VerifiedPubSub.Manifest`, `VerifiedPubSub`, and
`VerifiedPubSub.Subscriber` describe the `docs` option and the section.

## Left to the builder

- The names of the module attributes.
- How the render runs once when a module has both `use` lines, within the timing rule above.
- The line wrap of a long event list. Markdown renders it the same either way.
