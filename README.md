# VerifiedPubsub

Compile-time verified PubSub for Elixir — the idea behind verified routes, applied to
topics and events.

A broadcast and its handler are normally two string literals in two files with nothing
tying them together. Rename an event and you leave a dead handler behind; delete one and
a subscriber quietly stops mattering; add one and nothing tells you who should care.
`VerifiedPubsub` makes a registry the single source of truth and turns that drift into
compile-time failures.

## Installation

```elixir
def deps do
  [
    {:verified_pubsub, "~> 0.1.0"}
  ]
end
```

`:spark` is the only required dependency. `:phoenix_pubsub` is optional — add it if you
use the Phoenix adapter.

## Declare topics and events once

```elixir
defmodule MyApp.Topics do
  use VerifiedPubsub.Registry,
    adapter: VerifiedPubsub.Adapter.PhoenixPubSub,
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

`:campaigns` is an alias used to build function names; the string is the wire topic, so
renaming it never breaks a call site. `%{account_id}` marks a parameter, and the
parameter list is derived from the pattern rather than declared twice.

## Broadcast through generated functions

```elixir
MyApp.Topics.broadcast_campaigns_created!(%{account_id: id}, %{id: c.id, name: c.name})
```

A topic with no params takes only a payload: `MyApp.Topics.broadcast_system_alert!(payload)`.

To skip the sender — the usual fix for a LiveView that both writes to a topic and
subscribes to it, and would otherwise apply its own change twice — use the `_from`
variants:

```elixir
MyApp.Topics.broadcast_campaigns_created_from!(self(), %{account_id: id}, payload)
```

These mirror `Phoenix.PubSub.broadcast_from/4`, with `from` leading for the same reason
it does there, and they carry the same semantics: `from` is whichever pid you pass, so
`self()` is the calling process. If the broadcast happens inside a context function, a
`Task`, or an Oban job, `self()` is *that* process, not the one that started the
request — pass the pid explicitly in those cases.

Each event therefore generates four broadcast functions: `broadcast_*`,
`broadcast_*!`, `broadcast_*_from`, and `broadcast_*_from!`.

## Subscribe, and account for every event

```elixir
defmodule MyAppWeb.CampaignsLive do
  use MyAppWeb, :live_view
  use VerifiedPubsub.Subscriber, registry: MyApp.Topics, topics: [:campaigns]

  def mount(_params, _session, socket) do
    if connected?(socket) do
      :ok = subscribe_campaigns(%{account_id: socket.assigns.account.id})
    end

    {:ok, stream(socket, :campaigns, [])}
  end

  handle_message :campaigns, :created, payload, socket do
    {:noreply, stream_insert(socket, :campaigns, payload)}
  end

  ignore_message :campaigns, :deleted
end
```

`ignore_message/2` is not a convenience. Subscribers routinely care about a subset of a
topic's events, and without an explicit opt-out exhaustiveness would be unusable rather
than merely strict. It makes "I know about this event and don't care" a deliberate,
greppable statement.

To match on topic params, pattern match the whole message rather than the payload:

```elixir
handle_message :campaigns, :created,
               %VerifiedPubsub.Message{params: %{account_id: id}, payload: payload},
               socket do
  {:noreply, socket}
end
```

## What is and is not checked

**Hard compile errors:**

- a subscriber that does not account for every event on a topic it subscribes to
- a subscriber that handles an event the registry does not declare, or a topic not in
  its `:topics` list
- duplicate topics, duplicate events on one topic, and malformed topic patterns

**Compile warnings, promoted to errors by `mix compile --warnings-as-errors`:**

- broadcasting an unknown topic or event. This works by the generated function not
  existing, and Elixir reports an undefined remote function as a warning — one that
  usefully lists the valid alternatives. Run `--warnings-as-errors` in CI to make it
  binding. This is weaker than verified routes, which raises from a sigil macro; the
  trade is that broadcasts stay plain function calls, with no `require` at every call
  site.
- a params map with the wrong keys, when the map is a literal. Elixir's type inference
  catches it against the destructured function head. A dynamically-built map raises
  `FunctionClauseError` at runtime instead.

**Not checked:**

- **Payload shapes.** `field` declarations are parsed and readable through
  `VerifiedPubsub.Info`, but nothing validates a payload against them yet.
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

The generated clause is emitted at the `use` site, so your own `handle_info/2` clauses
are matched after it. Add a catch-all if your process receives other messages:

```elixir
def handle_info(_other, state), do: {:noreply, state}
```

## Transports

`VerifiedPubsub.Adapter.PhoenixPubSub` is the usual choice. `VerifiedPubsub.Adapter.Local`
delivers in-VM with `send/2`, needs no Phoenix, and is useful in tests:

```elixir
children = [VerifiedPubsub.Adapter.Local]
```

Implement `VerifiedPubsub.Adapter` for anything else.

## Formatting

Add this to your `.formatter.exs` so the DSL reads without parentheses:

```elixir
[
  import_deps: [:verified_pubsub]
]
```
