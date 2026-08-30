# Verified PubSub — Design

**Date:** 2026-08-30
**Status:** Approved for planning
**Scope:** Pass 1 of `verified_pubsub`, a general-purpose Hex library

## Purpose

Bring compile-time verification to Phoenix PubSub the way verified routes brought it
to routing. A single registry module declares every topic and every event on that
topic. Broadcasting an event that does not exist becomes a compile error, and a
subscriber that fails to account for a declared event fails to compile.

The problem this solves is drift. Today a broadcast and its handler are two string
literals in two files with nothing tying them together, so renaming an event or
deleting one leaves silently dead handlers and silently unhandled messages. Making
the registry the single source of truth turns both failures into compile-time errors.

## Goals

1. **Valid topic and event on broadcast.** Broadcasting an unknown topic, or an event
   not declared on that topic, is a hard compile error. (An earlier revision of this
   spec downgraded this to a warning, which was true of the generated-function design
   it then described; the atom-first macros restored it.)
2. **Subscriber exhaustiveness.** A module subscribing to a topic must account for
   every event declared on it, either by handling it or by explicitly ignoring it.
3. **No handling of undeclared events.** Handling an event that does not exist on a
   subscribed topic is a compile error, catching typos and events deleted from the
   registry.
4. **Payload shape enforcement.** Declared and introspectable in pass 1, enforced in
   pass 2. The design must not need restructuring to add it.

## Non-goals for pass 1

- Payload shape *enforcement* (declaration and introspection only).
- Phoenix Channels as a subscriber target.
- Import-style broadcaster functions (`broadcast_x!` unqualified).
- Runtime topic registration.
- **Phoenix's custom-dispatch cluster**: the `dispatcher` argument on broadcasts,
  `subscribe/3`'s `:metadata` option, and `unsubscribe_match/3`. These three exist
  together to support Channel and Presence fan-out, so they are omitted as a group —
  adding any one alone yields an argument nothing can consume. If Channels come into
  scope, they arrive together.

  One consequence to know when picking this up: `subscribe_*` currently accepts no
  options. Since the adapter layer was dropped, this is now a smaller change than first
  recorded: `subscribe_*` would simply pass an opts list through to
  `Phoenix.PubSub.subscribe/3`, with no behaviour signature to break.
- `local_broadcast` / `local_broadcast_from`. Deliberately deferred: they appear mostly
  inside Phoenix itself (Presence, Channel internals) rather than in application code,
  and adding them would take the generated broadcast functions per event from four to
  eight (local × from × bang). Purely additive if wanted later.
- `direct_broadcast` (targeting a named node).

## Audience and constraints

A general-purpose Hex library. No assumptions about consumer app structure, and
greenfield wire-format semantics are permitted — there is no existing hand-rolled
PubSub usage this must stay bit-compatible with.

Subscriber targets, in priority order: plain GenServers, LiveViews, and transient or
broadcast-only processes (e.g. Oban workers). Phoenix Channels are out of scope.

`phoenix_pubsub` is a **required** dependency. An adapter layer was specified initially
to keep it optional; that was reversed — see "No adapter layer" below.

## Decisions

Two design questions dominated, and both were resolved deliberately.

### Subscriber definition style

Exhaustiveness checking requires knowing, at compile time, which events a module
accounts for. There are three ways to learn that, and they trade off against each
other such that you get two of three among *no duplication*, *robustness*, and
*standard `handle_info` syntax*:

- **Macro-defined clauses** (chosen). Zero duplication, exact coverage information.
  Costs non-standard definition syntax.
- **`@handles` annotations on plain `handle_info`.** Idiomatic and robust, but the
  event is named twice, and annotation/pattern drift is only a runtime error.
- **AST inference over plain `handle_info`** (rejected). Feasible via
  `Module.get_definition/2`, but it breaks on bound variables, guard-based
  `event in [...]`, partial struct matches, clauses generated in comprehensions, and
  delegation to helpers. Each of those yields a false "unhandled event" error on
  correct code, which is the worst possible failure mode for a verification tool, and
  the fix is to add annotations — paying the annotation cost without the rigor.

The chosen macro still *produces* real `handle_info/2` clauses, so the behaviour,
dispatch, and stack traces are ordinary Elixir.

### DSL implementation: Spark

Spark 2.7.2 (released 2026-06-07, ~2M downloads) has **zero required dependencies** —
`igniter`, `sourceror`, and `jason` are all optional — so consumers of this library
inherit no dep tree, which was the main argument against it.

Spark earns its place on the registry specifically: nested `topic do message ... end`
is the canonical section/entity shape; option schema validation and error messages
with source locations come free; and `Spark.InfoGenerator` supplies the introspection
API. Because payload
shape enforcement is a stated goal, the registry will grow nested `field` entities
with types, defaults, and required flags — exactly where hand-rolled schema
validation becomes tedious.

Spark does **not** help with the subscriber half. `handle_message`, the accumulated
coverage attribute, and the `@before_compile` diff are hand-rolled regardless.

Consequently the subscriber consumes the registry only through `VerifiedPubSub.Info`,
never through Spark internals, so the DSL front-end stays replaceable.

## Architecture

```
lib/verified_pubsub.ex                          # overview moduledoc
lib/verified_pubsub/message.ex                  # the wire struct
lib/verified_pubsub/registry.ex                 # use Spark.Dsl entry point
lib/verified_pubsub/dsl.ex                      # Spark.Dsl.Extension
lib/verified_pubsub/dsl/topic.ex                # entity structs
lib/verified_pubsub/dsl/message.ex
lib/verified_pubsub/dsl/field.ex
lib/verified_pubsub/info.ex                     # InfoGenerator + richer accessors
lib/verified_pubsub/transformers/parse_params.ex
lib/verified_pubsub/api.ex                       # the call-site macros
lib/verified_pubsub/transformers/validate_topics.ex
lib/verified_pubsub/subscriber.ex               # __using__, handle_message, before_compile
lib/verified_pubsub/broadcast.ex                 # bang! helper, keeps generated code clean
```

Each unit has one job: the DSL extension parses, transformers derive, verifiers
validate the registry, `Info` is the only read interface, `Subscriber` handles
subscriber-side codegen and verification, and adapters isolate transport.

## Components

### 1. Registry DSL

```elixir
defmodule MyApp.Topics do
  use VerifiedPubSub.Registry,
    pubsub: MyApp.PubSub

  topic :campaigns, "accounts:%{account_id}:campaigns" do
    message :created do
      field :id, :string
      field :name, :string
    end

    message :deleted do
      field :id, :string
    end
  end
end
```

The containing section is declared `top_level?: true` so `topic` is usable at module
level with no wrapper block.

**The alias is separate from the wire pattern.** `:campaigns` determines generated
function names; the string determines the wire topic. Renaming the wire string never
breaks a call site.

**Params are derived, not declared.** A transformer parses `%{name}` occurrences out
of the pattern, producing `[:account_id]`. Declaring them separately would be a second
source of truth.

**Payload fields are nested entities**, not a `%{id: :string}` map literal. More
verbose, but it is the Spark idiom, gets option validation free, and has room for
`required: true` and `default:` when enforcement lands. Pass 1 parses and exposes
these without enforcing them.

### 2. Call-site API: atom-first macros

```elixir
defmodule MyApp.Campaigns do
  use VerifiedPubSub, registry: MyApp.Topics

  def create(attrs) do
    broadcast!(:campaigns, :created, %{account_id: attrs.account_id}, payload)
  end
end
```

Seven macros — `subscribe/2`, `unsubscribe/2`, `topic/2`, `broadcast/4`, `broadcast!/4`,
`broadcast_from/5`, `broadcast_from!/5` — imported by `use VerifiedPubSub, registry: ...`
and by `use VerifiedPubSub.Subscriber`.

**This replaces an earlier design in this spec**, which generated
`broadcast_campaigns_created!/2` and friends onto the registry module. That design was
built, tested, and then reversed. The reasons:

1. **Verification is a hard error rather than a warning.** The generated design relied
   on the function not existing, and Elixir reports an undefined remote function as a
   *warning* — binding only under `--warnings-as-errors`. A macro raises `CompileError`.
2. **The errors are better.** Elixir's suggester does string similarity on function
   names, capped at five and mixing arities. A macro knows the registry, so it can list
   a topic's declared events exactly, and say where a misplaced event actually lives:
   `:alert is declared on [:system], not :campaigns`. That second diagnostic is
   structurally impossible in the generated design.
3. **Surface area.** 22 generated functions for a two-topic, four-event registry; ~95
   for five topics and twenty events. Now a fixed seven macros, and the registry
   generates one function (`__verified_pubsub_name__/0`).
4. **Consistency.** `handle_message :campaigns, :created` was already atom-first, so the
   same pair of identifiers was expressed two different ways.

Why plain functions taking atoms cannot work — verified empirically on Elixir 1.20.4:
given `def broadcast(:campaigns, :created, %{account_id: id}, payload)`, a call to
`broadcast(:campaigns, :creatd, ...)` produces **no diagnostic at all**. Type inference
does not narrow across clause heads on a remote call. The same check gives up on the
params map too, which the single-shaped generated head had caught. So atom-first
requires macros; there is no function-based version that verifies anything.

The costs, accepted:

- Every calling module needs `use VerifiedPubSub, registry: ...`. Smaller in practice
  than it looks: modules that `use VerifiedPubSub.Subscriber` already have the import.
- Macros cannot be piped into, captured with `&`, or called via `apply/3`.
- A module binds exactly one registry; a second `use` with a different registry raises.
- No autocomplete-driven discovery of the event catalog. The registry is one file and is
  the actual source of truth, which softens this.
- Macro expansion failures are harder to debug than undefined functions.

**Params.** A map, not positional arguments: positional reads terser with one param but
invites ordering bugs at two or more, and a map is self-documenting. A **literal** params
map is validated at expansion time, naming both missing and unexpected keys. A map built
at runtime cannot be checked, and `Map.fetch!/2` raises `KeyError` for a missing key.

**Literal atoms required.** Topic and event must be literal atoms; anything else is a
`CompileError` explaining why. A topic chosen at runtime is therefore not supported —
the same restriction the generated functions had, so nothing was lost here.

### 3. Wire format

```elixir
%VerifiedPubSub.Message{
  registry: MyApp.Topics,
  topic: :campaigns,
  event: :created,
  params: %{account_id: "7"},
  payload: %{id: "c_1", name: "Fall drive"}
}
```

A struct rather than a tagged tuple, because topics are parameterized: a process
subscribed to several accounts must know which one fired.

**No `meta` field in pass 1.** Struct fields are additive — adding one later does not
break patterns that never mentioned it — so there is no forward-compatibility reason
to reserve a speculative open map now, and shipping one empty would invite consumers
to put app data in an unvalidated field. Candidates for if and when a concrete need
arrives: origin pid or node (so a subscriber can ignore its own broadcasts), trace
context propagation, and a payload version tag for rolling-deploy skew.

Of these, **origin-based self-filtering is the only one that would change the
broadcast API** rather than just the struct, since the caller must be able to identify
itself. It remains deferrable as an optional argument to `broadcast_*`, but it is the
one to decide deliberately rather than discover late.

### 4. Subscriber

```elixir
defmodule MyAppWeb.CampaignsLive do
  use MyAppWeb, :live_view
  use VerifiedPubSub.Subscriber, registry: MyApp.Topics, topics: [:campaigns]

  def mount(_params, _session, socket) do
    if connected?(socket) do
      subscribe_campaigns(%{account_id: socket.assigns.account.id})
    end

    {:ok, socket}
  end

  handle_message :campaigns, :created, payload, socket do
    {:noreply, stream_insert(socket, :campaigns, payload)}
  end

  ignore_message :campaigns, :deleted
end
```

`use VerifiedPubSub.Subscriber` imports the registry's `subscribe_*`/`unsubscribe_*`
functions, registers the accumulating coverage attribute, imports `handle_message` and
`ignore_message`, and installs `@before_compile`.

**`ignore_message/2` is load-bearing, not a convenience.** LiveViews routinely care
about a subset of a topic's events; without an explicit opt-out, exhaustiveness is
unusable rather than merely strict. It makes "I know about this event and do not care"
a deliberate, greppable statement — the same role `_ => {}` plays in a Rust match.

**Third argument.** Ordinarily a pattern matched against the `payload`. If it is
syntactically a `%VerifiedPubSub.Message{}` struct pattern, it matches the whole
message instead, giving access to `params`:

```elixir
handle_message :campaigns, :created,
               %VerifiedPubSub.Message{params: %{account_id: acct}, payload: p},
               state do
  ...
end
```

Detection is reliable because struct patterns are unmistakable in the AST.

**Codegen mechanics.** `handle_message` does *not* define `handle_info` inline. It
accumulates clause AST into a module attribute; `@before_compile` then emits all
private dispatch clauses grouped together, plus exactly one
`handle_info(%VerifiedPubSub.Message{} = msg, state)` clause that delegates to them.

Inline definition would interleave generated clauses with the user's own
`def handle_info`, tripping Elixir's "clauses with the same name and arity should be
grouped together" warning. Emitting a single generated `handle_info` clause avoids
this entirely. Call-site line metadata is propagated into the quoted bodies so stack
traces point at user code.

**Known caveat, to be documented:** a user-defined catch-all `handle_info(_msg, state)`
in the same module will shadow the generated clause, since generated clauses are
emitted last. Elixir's own grouping warning surfaces this in practice.

### 5. Where each check lives

| Check | Mechanism |
|---|---|
| Duplicate topics or events; malformed `%{param}` syntax; unknown options | Spark **Transformer** returning `{:error, Spark.Error.DslError}`, with `path:` and source annotation |
| Unknown topic or event on broadcast | `CompileError` from the macro, listing the topic's declared events |
| Wrong param key on broadcast | `CompileError` for a literal map, naming missing and unexpected keys; `KeyError` for a dynamic map |
| Subscriber exhaustiveness and undeclared events | Hand-rolled `@before_compile` diff |

**Registry checks use Transformers, not Verifiers — verified empirically.** Spark's
documentation advises preferring Verifiers for pure validation, but a Verifier that
returns `{:error, _}` does **not** fail compilation: it runs via `@after_verify`, so
the error is printed as a *warning*, the module is still defined, and
`Kernel.ParallelCompiler.compile/1` returns `:ok`. A Transformer returning
`{:error, Spark.Error.DslError}` raises a hard compile error and the module is never
defined, which is what "is a compile error" in the Goals requires. Spark's advice
exists to avoid cross-module compile-time dependencies; our registry checks reference
no other modules, so a Transformer carries no such risk.

The same finding means **Spark's built-in duplicate detection is not sufficient**.
`Spark.Dsl.Verifiers.VerifyEntityUniqueness` catches duplicate top-level entities
(two `topic :campaigns`) but, being a Verifier, only warns — and it does not check
nested entities at all, so two `message :created` blocks inside one topic pass
silently. Both duplicate topics and duplicate events must be checked in our own
Transformer.

The subscriber side is separate: it calls `VerifiedPubSub.Info.events/2` at compile
time, which creates a compile-time dependency on the registry, so **editing the
registry recompiles every subscriber**.

That is the property we want: deleting an event immediately breaks its handlers. The
accepted cost is recompilation fan-out, the same bargain the old Phoenix router
helpers made.

The `@before_compile` diff, per subscribed topic:

- `declared` — from `Info.events/2`
- `accounted_for` — accumulated from `handle_message` and `ignore_message`
- `MapSet.difference(declared, accounted_for)` → missing, reported per `on_missing`
- `MapSet.difference(accounted_for, declared)` → undeclared, always an error

**Set semantics, not list subtraction.** Multiple `handle_message` clauses for the
same `{topic, event}` are legal and expected (see param matching below), so Elixir's
`--`, which removes only one occurrence per element, would leave a residue and report
a correctly-handled event as undeclared.

`on_missing: :error | :warn | :ignore` defaults to `:error`. Errors are raised as
`CompileError` naming the module, the missing events, and the `ignore_message` escape
hatch.

**Matching on param values, and what verification does not cover.** Because
`handle_message` expands to a real `handle_info/2` clause, the `%Message{}` pattern
form can match `params` at runtime like any other pattern, including several clauses
per `{topic, event}` with different param patterns:

```elixir
handle_message :campaigns, :created,
               %VerifiedPubSub.Message{params: %{account_id: "7"}, payload: p},
               state do
```

This is strictly more than plain Phoenix PubSub offers, where the topic string is not
carried in the message at all and params must be hand-copied into the payload.

The limit is that **coverage is tracked per `{topic, event}` pair, not per param
value.** If every clause for an event matches a narrow param value, the event counts
as covered while a message with any other param value falls through to a
`FunctionClauseError`. This gap is inherent — it is value coverage, not name coverage,
and no static check closes it. It is rarely felt in practice, since a process
subscribes with concrete params and therefore already knows them; it matters only when
one process subscribes to several instances of a parameterized topic. Documentation
should recommend a final param-agnostic clause when a subscriber does match on param
values.

### 6. No adapter layer

Generated functions call `Phoenix.PubSub` directly. `use VerifiedPubSub.Registry` takes
a required `pubsub:` naming a `Phoenix.PubSub` process; transport is configured there.

This reverses an earlier decision in this spec, which specified a
`VerifiedPubSub.Adapter` behaviour with `PhoenixPubSub` and `Local` implementations.
Three facts killed it:

1. **`Phoenix.PubSub` already has its own adapter behaviour** (`node_name/1`,
   `child_spec/1`, `broadcast/4`, `direct_broadcast/5`) — the documented extension point
   for PG2, Redis, and anything else. Layering a second adapter concept over it
   duplicates an extension point one level down and splits transport configuration
   across two places.
2. **`phoenix_pubsub` has zero transitive dependencies.** "Keep Phoenix optional," the
   original justification, was asserted without checking this. Requiring it costs
   essentially nothing.
3. **`Adapter.Local` was a reimplementation.** `Phoenix.PubSub.subscribe/3` is literally
   `Registry.register(pubsub, topic, opts[:metadata])` — the same mechanism `Local` used.

What is lost: the library no longer runs on Spark alone, and a non-web OTP app inherits
a package named `phoenix_*`. Both were judged cosmetic against a wrapper whose only real
implementation was four pass-through lines — an abstraction with a single implementation
is validated by nothing.

Tests start a real `Phoenix.PubSub`, which is also the production code path.

One nuance worth recording: `Phoenix.PubSub`'s own adapter behaviour covers **only
cross-node propagation** — it has no subscribe/unsubscribe, which `Phoenix.PubSub`
handles itself. So the rejected behaviour was not literally a duplicate of it; it was a
wrapper around the whole of `Phoenix.PubSub`. The objection stands either way.

## Error handling

- **Registry errors** are `Spark.Error.DslError` with a `path:` such as
  `[:topics, :campaigns, :created]`, carrying Spark's source annotation.
- **Subscriber errors** are `CompileError` raised from `@before_compile`, located at
  the `use VerifiedPubSub.Subscriber` call site, listing every missing or undeclared
  event and naming `ignore_message/2`.
- **Broadcast failures** raise from `broadcast_*!` and are returned as
  `{:error, term}` from `broadcast_*`.

Error message quality is a primary feature, not polish. A verification tool whose
errors do not say what to do is worse than no tool.

## Testing strategy

The product is compile-time behaviour, so the central tests assert on compilation
outcomes. This needs a `compile_string!/1` test helper that compiles a module source
in-process and captures raised errors and emitted warnings.

1. **DSL parsing** — build registries, assert `Info` returns the expected topics,
   events, params, and fields.
2. **Registry transformers** — compile deliberately-broken registries with
   `Code.compile_string/1` and `assert_raise Spark.Error.DslError`, checking message
   content (duplicate topic, duplicate event, malformed param). This works precisely
   because the checks are Transformers; the same assertions against a Verifier would
   silently pass while only emitting a warning.
3. **Subscriber verification** — the crux. Assert `CompileError` for a missing event
   and for an undeclared event; assert `ignore_message` satisfies coverage; assert
   `on_missing: :warn` warns rather than raises.
4. **Broadcast param handling** — assert the params map interpolates into the correct
   topic string, that a literal map with wrong keys fails to compile, and that a map
   built at runtime with a missing key raises `KeyError`.
5. **Integration** — a real GenServer subscriber over a real `Phoenix.PubSub`; assert
   delivery and that the right clause runs.
6. **LiveView smoke test** — one test behind a test-only `phoenix_live_view` dep,
   confirming `use VerifiedPubSub.Subscriber` composes with `use Phoenix.LiveView`
   and that messages reach the generated `handle_info`.

## Delivery notes

- Add `mix spark.formatter` integration so `locals_without_parens` for the Spark
  entities (`topic`, `message`, `field`) is generated rather than hand-maintained, and
  export it in `.formatter.exs` for consumers. `handle_message` and `ignore_message`
  are ordinary macros, not Spark entities, so they must be added to the exported
  `locals_without_parens` by hand.
- `lib/verified_pubsub.ex`'s current skeleton (`topic/1`, `message/2`, empty
  `compile/1`) is superseded by this design and will be replaced.
- `mix.exs` needs `elixir: "~> 1.17"` retained, `{:spark, "~> 2.7"}`,
  `{:phoenix_pubsub, "~> 2.1"}`, and test-only `phoenix_live_view`.
