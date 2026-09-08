# VerifiedPubSub

Compile-time verified PubSub for Elixir — the idea behind verified routes, applied to
topics and events.

A broadcast and its handler are normally two string literals in two files with nothing
tying them together. Rename an event and you leave a dead handler behind; delete one and
a subscriber quietly stops mattering; add one and nothing tells you who should care.
`VerifiedPubSub` makes a manifest the single source of truth and turns that drift into
compile-time failures.

## Installation

```elixir
def deps do
  [
    {:verified_pubsub, "~> 0.1.0"}
  ]
end
```

Requires `:spark` and `:phoenix_pubsub`, neither of which has any transitive dependencies
of its own. Start a `Phoenix.PubSub` in your supervision tree as you normally would, and
add the formatter import so the DSL reads without parentheses:

```elixir
# application.ex
children = [{Phoenix.PubSub, name: MyApp.PubSub}]

# .formatter.exs
[import_deps: [:verified_pubsub]]
```

Not published yet, so there are no HexDocs. `mix docs` builds the full documentation
locally; every module referenced below carries a detailed `@moduledoc`.

## Declare topics and events once

```elixir
defmodule MyApp.Topics do
  use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

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

`:campaigns` is the name call sites use; the string is the wire topic, so renaming the
pattern never touches a call site. `%{account_id}` marks a parameter, and the parameter
list is derived from the pattern rather than declared twice.

## Broadcast

```elixir
defmodule MyApp.Campaigns do
  use VerifiedPubSub, manifest: MyApp.Topics

  def create(attrs) do
    with {:ok, campaign} <- insert(attrs) do
      broadcast!(:campaigns, %{account_id: campaign.account_id}, :created, %{campaign: campaign})
      {:ok, campaign}
    end
  end
end
```

A topic is always followed immediately by its params, in every macro here. Params exist
only to fill in the topic pattern, so `{topic, params}` is the address and
`{event, payload}` is the message — the address-then-message shape of
`Phoenix.PubSub.broadcast/3`. A topic with no params takes no params argument; omitting
them on a topic that *does* take them is a compile error naming them.

```elixir
broadcast!(:system, :alert, payload)
subscribe(:system)
```

These are **macros**, so the topic and event must be literal atoms — that is what lets a
typo fail the compile. Params and payloads may be built at runtime.

To skip the sender — the usual fix for a LiveView that both writes to a topic and
subscribes to it, and would otherwise apply its own change twice:

```elixir
broadcast_from!(self(), :campaigns, %{account_id: id}, :created, payload)
```

This mirrors `Phoenix.PubSub.broadcast_from/4` and carries the same semantics: `from` is
whichever pid you pass, so `self()` is the *calling* process. Inside a context function, a
`Task`, or an Oban job that is *that* process, not the one that started the request — pass
the pid explicitly there.

## Why macros rather than functions

Plain functions taking atoms cannot be verified at compile time. Elixir's type inference
does not narrow across clause heads on a remote call, so a `broadcast/4` defined as

```elixir
def broadcast(:campaigns, %{account_id: id}, :created, payload), do: ...
```

produces **no diagnostic at all** for `broadcast(:campaigns, params, :creatd, ...)` —
verified empirically on Elixir 1.20.4. It fails at runtime instead. A macro can look the
topic and event up in the manifest while your code compiles.

The costs are real: every calling module needs `use VerifiedPubSub, manifest: ...`, and
macros cannot be piped into, captured with `&`, or called via `apply/3`. Modules that
`use VerifiedPubSub.Subscriber` already have the import.

## Subscribe, and account for every event

```elixir
defmodule MyAppWeb.CampaignsLive do
  use MyAppWeb, :live_view
  use VerifiedPubSub.Subscriber, manifest: MyApp.Topics

  def mount(_params, _session, socket) do
    if connected?(socket) do
      :ok = subscribe(:campaigns, %{account_id: socket.assigns.account.id})
    end

    {:ok, stream(socket, :campaigns, [])}
  end

  handle_message :campaigns, :created, payload, socket do
    {:noreply, stream_insert(socket, :campaigns, payload)}
  end

  ignore_message :campaigns, [:updated, :deleted]
end
```

The topics a module subscribes to are **inferred** from its `handle_message` and
`ignore_message` calls, so there is no list to keep in sync with them.

`ignore_message/2` is not a convenience. Subscribers routinely care about a subset of a
topic's events, and without an explicit opt-out exhaustiveness would be unusable rather
than merely strict. It makes "I know about this event and don't care" a deliberate,
greppable statement.

To match on topic params, put them where a broadcast takes them. The leading arguments are
the same in both — a broadcast builds them, a handler matches them:

```elixir
broadcast!     :campaigns, %{account_id: id},   :created, payload
handle_message :campaigns, %{account_id: acct}, :created, payload, socket
```

A literal there narrows the clause to one param value, and ordinary clause ordering
applies. Unlike a broadcast, the pattern may name a **subset** of the params. The params
argument is optional, so `handle_message :campaigns, :created, payload, socket` still
works, as does matching the whole `%VerifiedPubSub.Message{}` in the payload position.

## Payload shapes

Each `field` declares a key the payload must carry:

```elixir
message :created do
  field :id, :string
  field :campaign, MyApp.Campaign
  field :tags, {:list, :string}
  field :note, :string, required: false
end
```

A type is one of `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list`,
`:any`, a `{:list, type}` tuple, or a struct module. An unknown type is a compile error.
`required: false` allows the key to be absent, or present as `nil`.

**The payload is a map of exactly the declared fields.** Undeclared keys are rejected, so
the manifest stays an accurate description of what is on the wire. A struct therefore
cannot be the payload itself — it carries `__struct__` and all its own keys — so put it in
a field, as `%{campaign: campaign}` does above.

Errors report every problem at once rather than the first:

```
invalid payload for :campaigns :created in MyApp.Topics:

  * missing required key: :id
  * unexpected key: :extra — not declared on this event
  * :name is declared as :string, got: 42
```

## Topic patterns

Topics are `:`-delimited, and each `%{param}` must fill a whole segment. So
`"accounts:%{account_id}:campaigns"` is fine and `"accounts:acct%{account_id}"` is a
compile error. `~p` restricts path interpolation the same way, for the same reason.

Three rules together guarantee an interpolated topic can only be the topic its call site
names — the first two checked when the manifest compiles, the third on every call:

1. every `%{param}` fills a whole segment
2. no two declared patterns can match the same wire topic
3. a param value is non-empty and contains no `:`

Drop any one and the guarantee fails. `"a:%{x}"` and `"a:%{x}:b"` differ in segment count,
so rule 2 holds, yet `x = "1:b"` on the first builds `"a:1:b"` — exactly what the second
builds from `x = "1"`. Without rule 3, a broadcast on one topic reaches the other's
subscribers.

Values are not escaped, because a topic string is a wire format that other systems may
also subscribe to; silently rewriting it would be worse than refusing.

## What is and is not checked

**Hard compile errors:**

- an unknown topic or event, or an event belonging to a different topic — the error names
  the declared events, and where a misplaced event actually lives
- a literal params or payload map with missing or unexpected keys
- a subscriber that does not account for every event on a topic it subscribes to, or that
  names a topic or event the manifest does not declare
- a handler pattern that could never match — a param the topic does not declare, or a
  payload key the event does not declare
- duplicate topics, duplicate events, and malformed or overlapping topic patterns

**Checked at runtime, on every broadcast, in every environment:**

- payload shape — `VerifiedPubSub.PayloadError`, raised from `broadcast/4` as well as
  `broadcast!/4`, since a shape violation is a bug rather than a transport blip a caller
  might reasonably handle
- topic param values — `VerifiedPubSub.TopicError`
- the sending manifest — `VerifiedPubSub.ManifestMismatchError`

**Not checked:**

- **A params map built at runtime.** `Map.fetch!/2` raises `KeyError` for a missing key
  instead.
- **Topic param values.** Coverage is tracked per `{topic, event}` pair. If every clause
  for an event matches a narrow param value, the event still counts as covered, and a
  message with a different value raises `FunctionClauseError`. End with a param-agnostic
  clause when matching on param values.

## Messages your process does not expect

Defining any `handle_info/2` discards the default that `use GenServer` and
`use Phoenix.LiveView` install, so an unexpected message raises `FunctionClauseError`
rather than being logged. That is normal for any GenServer with a custom
`handle_info/2`, and it cannot be avoided: Elixir 1.20 made `super/2` for GenServer
callbacks a hard error, so the default body is unreachable.

The generated clauses are emitted at the `use` site, so your own `handle_info/2` clauses
are matched after them. Add a catch-all if your process receives other messages:

```elixir
def handle_info(_other, state), do: {:noreply, state}
```

## Several manifests in one application

Nothing stops an application declaring a manifest per context, and a module is bound to
exactly one — binding a second is a compile error.

Isolation between them is *not* automatic. Rule 2 above is checked *within* a manifest,
since a manifest cannot see its siblings while it compiles — so two that share a `:pubsub`
can build the same wire topic. `Phoenix.PubSub` then delivers the message, its topic and
event atoms may match a clause in the wrong subscriber, and its payload follows the
*sender's* declarations.

A subscriber therefore refuses any message from a manifest other than its own, raising
`VerifiedPubSub.ManifestMismatchError`. That clause precedes your own `handle_info/2`, so
a catch-all does not absorb it — a silently swallowed collision would never be found.

For hard isolation, give each manifest its own `Phoenix.PubSub`; separate instances are
separate registries, so a colliding topic is never delivered at all:

```elixir
children = [
  Supervisor.child_spec({Phoenix.PubSub, name: Campaigns.PubSub}, id: :campaigns_pubsub),
  Supervisor.child_spec({Phoenix.PubSub, name: Accounts.PubSub}, id: :accounts_pubsub)
]
```

Which modules may bind which manifest is an ordinary dependency-boundary question — a
manifest is just a module, so `mix xref` or the `boundary` package enforces it better than
this library could.

## Transport

Everything goes through `Phoenix.PubSub`. There is deliberately no adapter layer:
`Phoenix.PubSub` already has its own adapter behaviour — that is where PG2, Redis, and
anything else get configured — so wrapping it would duplicate an extension point one
layer down and split transport configuration across two places.

In tests, start a `Phoenix.PubSub` as you would in production, and assert on delivery by
subscribing from the test process:

```elixir
use VerifiedPubSub, manifest: MyApp.Topics

start_supervised!({Phoenix.PubSub, name: MyApp.PubSub})
:ok = subscribe(:campaigns, %{account_id: id})
:ok = broadcast!(:campaigns, %{account_id: id}, :created, %{id: "c1"})
assert_receive %VerifiedPubSub.Message{event: :created}
```
