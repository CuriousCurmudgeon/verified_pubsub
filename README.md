# VerifiedPubSub

Compile-time verified PubSub for Elixir — the idea behind verified routes, applied to
topics and events.

A broadcast and its handler are normally two string literals in two files with nothing
tying them together. Rename an event and you leave a dead handler behind; delete one and
a subscriber quietly stops mattering; add one and nothing tells you who should care.
`VerifiedPubSub` makes a registry the single source of truth and turns that drift into
compile-time failures.

## Installation

```elixir
def deps do
  [
    {:verified_pubsub, "~> 0.1.0"}
  ]
end
```

Requires `:spark` and `:phoenix_pubsub`, neither of which has any transitive
dependencies of its own.

## Declare topics and events once

```elixir
defmodule MyApp.Topics do
  use VerifiedPubSub.Registry, pubsub: MyApp.PubSub

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

`:campaigns` is an alias used to build function names; the string is the wire topic, so
renaming it never breaks a call site. `%{account_id}` marks a parameter, and the
parameter list is derived from the pattern rather than declared twice.

## Broadcast with verified topics and events

```elixir
defmodule MyApp.Campaigns do
  use VerifiedPubSub, registry: MyApp.Topics

  def create(attrs) do
    with {:ok, campaign} <- insert(attrs) do
      broadcast!(:campaigns, :created, %{account_id: campaign.account_id}, %{campaign: campaign})
      {:ok, campaign}
    end
  end
end
```

`use VerifiedPubSub, registry: ...` imports the API. A topic with no params takes an
empty map: `broadcast!(:system, :alert, %{}, payload)`.

These are **macros**, so the topic and event must be literal atoms — that is what lets a
typo fail the compile. Params may be built at runtime.

To skip the sender — the usual fix for a LiveView that both writes to a topic and
subscribes to it, and would otherwise apply its own change twice:

```elixir
broadcast_from!(self(), :campaigns, :created, %{account_id: id}, payload)
```

This mirrors `Phoenix.PubSub.broadcast_from/4`, with `from` leading for the same reason
it does there, and it carries the same semantics: `from` is whichever pid you pass, so
`self()` is the calling process. If the broadcast happens inside a context function, a
`Task`, or an Oban job, `self()` is *that* process, not the one that started the
request — pass the pid explicitly in those cases.

## Why macros rather than functions

Plain functions taking atoms cannot be verified at compile time. Elixir's type inference
does not narrow across clause heads on a remote call, so a `broadcast/4` defined as

```elixir
def broadcast(:campaigns, :created, %{account_id: id}, payload), do: ...
```

produces **no diagnostic at all** for `broadcast(:campaigns, :creatd, ...)` — it fails at
runtime. Macros can look the topic and event up in the registry while your code compiles.

The costs are real: every calling module needs `use VerifiedPubSub, registry: ...`, and
macros cannot be piped into, captured with `&`, or called via `apply/3`. Modules that
`use VerifiedPubSub.Subscriber` already have the import.

## Subscribe, and account for every event

```elixir
defmodule MyAppWeb.CampaignsLive do
  use MyAppWeb, :live_view
  use VerifiedPubSub.Subscriber, registry: MyApp.Topics

  def mount(_params, _session, socket) do
    if connected?(socket) do
      :ok = subscribe(:campaigns, %{account_id: socket.assigns.account.id})
    end

    {:ok, stream(socket, :campaigns, [])}
  end

  handle_message :campaigns, :created, payload, socket do
    {:noreply, stream_insert(socket, :campaigns, payload)}
  end

  ignore_message :campaigns, :deleted
end
```

The topics a module subscribes to are **inferred** from its `handle_message`/`ignore_message`
calls, so there is no list to keep in sync with them.

`ignore_message/2` is not a convenience. Subscribers routinely care about a subset of a
topic's events, and without an explicit opt-out exhaustiveness would be unusable rather
than merely strict. It makes "I know about this event and don't care" a deliberate,
greppable statement. It also takes a list:

```elixir
ignore_message :campaigns, [:updated, :deleted]
```

To match on topic params, pattern match the whole message rather than the payload:

```elixir
handle_message :campaigns, :created,
               %VerifiedPubSub.Message{params: %{account_id: id}, payload: payload},
               socket do
  {:noreply, socket}
end
```

## What is and is not checked

**Hard compile errors:**

- broadcasting an unknown topic, an unknown event, or an event that belongs to a
  different topic — the error names the topic's declared events, and says where a
  misplaced event actually lives
- a literal params map with missing or unexpected keys
- a subscriber that does not account for every event on a topic it subscribes to
- a subscriber that handles an event the registry does not declare, or names a topic the
  registry does not declare
- duplicate topics, duplicate events on one topic, and malformed topic patterns
- a literal payload map that does not match the declared fields

**Checked at runtime, on every broadcast, in every environment:**

- payload shape — missing required keys, undeclared keys, and field types. Raises
  `VerifiedPubSub.PayloadError` from both `broadcast/4` and `broadcast!/4`, because a
  shape violation is a bug in the calling code rather than something a caller should
  handle like a network blip.

**Not checked:**

- **A params map built at runtime.** `Map.fetch!/2` raises `KeyError` for a missing key
  instead.
- **Topic param values.** Coverage is tracked per `{topic, event}` pair. If every clause
  for an event matches a narrow param value, the event still counts as covered, and a
  message with a different value raises `FunctionClauseError`. End with a param-agnostic
  clause when matching on param values.

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

**The payload is a map of exactly the declared fields.** Undeclared keys are rejected, so
the registry stays an accurate description of what is on the wire. That also means a
struct cannot be the payload itself — it carries `__struct__` and all of its own keys — so
put it in a field:

```elixir
broadcast!(:campaigns, :created, %{account_id: id}, %{campaign: campaign})
```

`required: false` allows the key to be absent, or present as `nil`.

Errors report every problem at once rather than the first:

```
invalid payload for :campaigns :created in MyApp.Topics:

  * missing required key: :id
  * unexpected key: :extra — not declared on this event
  * :name is declared as :string, got: 42
```

## Messages your process does not expect

Defining any `handle_info/2` discards the default that `use GenServer` and
`use Phoenix.LiveView` install, so an unexpected message raises `FunctionClauseError`
rather than being logged. That is normal for any GenServer with a custom
`handle_info/2`, and it cannot be avoided: Elixir 1.20 made `super/2` for GenServer
callbacks a hard error, so the default body is unreachable.

The generated clause is emitted at the `use` site, so your own `handle_info/2` clauses
are matched after it. Add a catch-all if your process receives other messages:

```elixir
def handle_info(_other, state), do: {:noreply, state}
```

## Transport

Broadcasts and subscriptions go through `Phoenix.PubSub`, so `:pubsub` names one started
in your supervision tree:

```elixir
children = [{Phoenix.PubSub, name: MyApp.PubSub}]
```

There is deliberately no adapter layer. `Phoenix.PubSub` already has its own adapter
behaviour — that is where PG2, Redis, and anything else get configured — so wrapping it
would duplicate an extension point one layer down and split transport configuration
across two places.

In tests, start a `Phoenix.PubSub` as you would in production, and assert on delivery by
subscribing from the test process:

```elixir
use VerifiedPubSub, registry: MyApp.Topics

start_supervised!({Phoenix.PubSub, name: MyApp.PubSub})
:ok = subscribe(:campaigns, %{account_id: id})
:ok = broadcast!(:campaigns, :created, %{account_id: id}, %{id: "c1"})
assert_receive %VerifiedPubSub.Message{event: :created}
```

## Formatting

Add this to your `.formatter.exs` so the DSL reads without parentheses:

```elixir
[
  import_deps: [:verified_pubsub]
]
```
