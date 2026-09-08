# VerifiedPubSub

Compile-time verified PubSub for Elixir — the idea behind verified routes, applied to
topics and events. Declare your topics and events once, and a typo becomes a compile
error instead of a message nobody receives.

## Installation

```elixir
def deps do
  [
    {:verified_pubsub, "~> 0.1.0"}
  ]
end
```

Start a `Phoenix.PubSub` in your supervision tree:

```elixir
children = [{Phoenix.PubSub, name: MyApp.PubSub}]
```

And add the formatter import, so the DSL reads without parentheses:

```elixir
# .formatter.exs
[import_deps: [:verified_pubsub]]
```

## Declare your topics and events

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

`:campaigns` is the name call sites use, and the string is the wire topic.
`%{account_id}` marks a parameter.

## Broadcast

```elixir
defmodule MyApp.Campaigns do
  use VerifiedPubSub, manifest: MyApp.Topics

  def create(attrs) do
    with {:ok, campaign} <- insert(attrs) do
      broadcast!(:campaigns, %{account_id: campaign.account_id}, :created, %{
        id: campaign.id,
        name: campaign.name
      })

      {:ok, campaign}
    end
  end
end
```

To skip the sender, which a LiveView that both writes and subscribes usually wants:

```elixir
broadcast_from!(self(), :campaigns, %{account_id: id}, :created, payload)
```

## Subscribe

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

  ignore_message :campaigns, :deleted
end
```

Every event on a subscribed topic must be handled or explicitly ignored. To match on the
topic params, put them where a broadcast takes them:

```elixir
handle_message :campaigns, %{account_id: acct}, :created, payload, socket do
  {:noreply, assign(socket, :account_id, acct)}
end
```

## Compile-time errors

A typo'd event:

```elixir
broadcast!(:campaigns, %{account_id: id}, :creatd, %{id: "c1", name: "n"})
```

```
** (CompileError) unknown event :creatd on topic :campaigns.

Declared events: [:created, :deleted]
```

A subscriber that does not account for every event:

```elixir
handle_message :campaigns, :created, payload, socket do
  {:noreply, socket}
end
```

```
** (CompileError) MyAppWeb.CampaignsLive subscribes to topics with events it does not
account for:

  * :campaigns, :deleted

Add a `handle_message` clause for each, or dismiss it explicitly:

    ignore_message :campaigns, :deleted

Manifest: MyApp.Topics
```

Unknown topics, mistyped params, and literal payloads that don't match the declared
fields all fail the same way.

## Runtime errors

A payload built at runtime is checked on every broadcast, in every environment:

```elixir
broadcast!(:campaigns, %{account_id: id}, :created, %{id: 42, extra: true})
```

```
** (VerifiedPubSub.PayloadError) invalid payload for :campaigns :created in MyApp.Topics:

  * missing required key: :name
  * unexpected key: :extra — not declared on this event
  * :id is declared as :string, got: 42
```

## Documentation

`mix docs` builds the full documentation locally.
