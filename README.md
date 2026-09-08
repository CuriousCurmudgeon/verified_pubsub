# VerifiedPubSub

Compile-time verified PubSub for Elixir — the idea behind verified routes, applied to
topics and events.

A broadcast and its handler are normally two string literals in two files with nothing
tying them together. Rename an event and you leave a dead handler behind; delete one and
a subscriber quietly stops mattering; add one and nothing tells you who should care.
`VerifiedPubSub` makes a manifest the single source of truth and turns that drift into
compile-time failures.

📖 **[Full documentation on HexDocs](https://hexdocs.pm/verified_pubsub)**

## Installation

```elixir
def deps do
  [
    {:verified_pubsub, "~> 0.1.0"}
  ]
end
```

Requires `:spark` and `:phoenix_pubsub`, neither of which has any transitive dependencies
of its own. Start a `Phoenix.PubSub` in your supervision tree as you normally would:

```elixir
children = [{Phoenix.PubSub, name: MyApp.PubSub}]
```

Then add this to `.formatter.exs` so the DSL reads without parentheses:

```elixir
[
  import_deps: [:verified_pubsub]
]
```

## Declare topics and events once

```elixir
defmodule MyApp.Topics do
  use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

  topic :campaigns, "accounts:%{account_id}:campaigns" do
    message :created do
      field :id, :string
      field :name, :string
      field :note, :string, required: false
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

Each `field` declares a key the payload must carry — the payload is a map of exactly the
declared fields. See [`VerifiedPubSub.Manifest`][manifest] for the field types and
[topic pattern rules][topic].

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

A topic is always followed immediately by its params, so `{topic, params}` is the address
and `{event, payload}` is the message. A topic with no params takes no params argument:

```elixir
broadcast!(:system, :alert, payload)
subscribe(:system)
```

These are **macros**, so the topic and event must be literal atoms — that is what lets a
typo fail the compile. Params and payloads may be built at runtime.

See [`VerifiedPubSub.Api`][api] for the full surface, `broadcast_from!/5`, and why the
call site is macros rather than functions.

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
`ignore_message` calls, so there is no list to keep in sync. Every event on those topics
must then be handled or explicitly dismissed, or the module does not compile.
`ignore_message` is mandatory rather than a convenience: it makes "I know about this event
and don't care" a deliberate, greppable statement.

To match the topic params, put them where a broadcast takes them. The leading arguments
are the same in both — a broadcast builds them, a handler matches them:

```elixir
broadcast!     :campaigns, %{account_id: id},   :created, payload
handle_message :campaigns, %{account_id: acct}, :created, payload, socket
```

See [`VerifiedPubSub.Subscriber`][subscriber] for the generated code, partial param
matching, catch-alls, and multi-manifest applications.

## What is and is not checked

**Compile errors** — an unknown topic or event, or an event belonging to a different
topic; a literal params or payload map with missing or unexpected keys; a subscriber that
doesn't account for every event on a topic it subscribes to, or names something the
manifest doesn't declare; a handler pattern that could never match; duplicate or
malformed declarations.

**Every broadcast, in every environment** — payload shape raises
[`VerifiedPubSub.PayloadError`][payload_error] (from `broadcast/4` as well as
`broadcast!/4`, since a shape violation is a bug rather than a transport blip); a topic
param value that is empty or contains `:` raises [`VerifiedPubSub.TopicError`][topic]; a
message from another manifest raises
[`VerifiedPubSub.ManifestMismatchError`][mismatch_error].

**Not checked** — a params map built at runtime (`Map.fetch!/2` raises `KeyError`
instead), and topic param *values*: coverage is tracked per `{topic, event}`, so a clause
matching a narrow param value still counts as covering the event, and another value
raises `FunctionClauseError`. End with a param-agnostic clause when matching on params.

## Testing

Start a `Phoenix.PubSub` as you would in production and assert on delivery by subscribing
from the test process:

```elixir
use VerifiedPubSub, manifest: MyApp.Topics

start_supervised!({Phoenix.PubSub, name: MyApp.PubSub})
:ok = subscribe(:campaigns, %{account_id: id})
:ok = broadcast!(:campaigns, %{account_id: id}, :created, %{id: "c1"})
assert_receive %VerifiedPubSub.Message{event: :created}
```

## Transport

Everything goes through `Phoenix.PubSub`. There is deliberately no adapter layer:
`Phoenix.PubSub` already has its own adapter behaviour — that is where PG2, Redis and
anything else get configured — so wrapping it would duplicate an extension point one
layer down and split transport configuration across two places.

[manifest]: https://hexdocs.pm/verified_pubsub/VerifiedPubSub.Manifest.html
[api]: https://hexdocs.pm/verified_pubsub/VerifiedPubSub.Api.html
[subscriber]: https://hexdocs.pm/verified_pubsub/VerifiedPubSub.Subscriber.html
[topic]: https://hexdocs.pm/verified_pubsub/VerifiedPubSub.Topic.html
[payload_error]: https://hexdocs.pm/verified_pubsub/VerifiedPubSub.PayloadError.html
[mismatch_error]: https://hexdocs.pm/verified_pubsub/VerifiedPubSub.ManifestMismatchError.html
