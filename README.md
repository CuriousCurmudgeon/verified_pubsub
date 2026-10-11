# VerifiedPubSub

Compile-time verified PubSub for Elixir — the idea behind verified routes, applied to
topics and events. Declare your topics and events once, and a typo becomes a compile
error instead of a message nobody receives.

## Installation

With [Igniter](https://hexdocs.pm/igniter):

```sh
mix igniter.install verified_pubsub
```

This adds the formatter import, finds the `Phoenix.PubSub` your application starts (or
adds one), and creates an empty `MyApp.Topics` manifest that uses it.

To install by hand, add the dependency:

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

  topic :pets, ["owners", :owner_id, "pets"] do
    message :adopted do
      field :id, :string
      field :name, :string
    end

    message :rehomed do
      field :id, :string
    end
  end

  topic :treats, ["owners", :owner_id, "treats"] do
    message :given do
      field :pet_id, :string
      field :from_owner_id, :string
      field :treat, :atom
    end
  end
end
```

`:pets` is the name call sites use, and the list is the wire topic: its segments are
joined with `:`, so this one is `"owners:<owner_id>:pets"`. A string is a literal
segment and an atom is a parameter.

Each `message` declares one event on that topic, and its `field`s declare the keys that
event's payload must carry. A payload is a map of exactly those fields — add
`required: false` to a field to make it optional.

## Document your topics

The manifest documents itself. Its `@moduledoc` gains a `## Topics` section listing each
topic's wire pattern, its events, and each event's fields and types, so the docs cannot
drift from what the manifest enforces. Add a `description` to say what a topic, event or
field means — inside the block, or as a keyword when there is no block:

```elixir
topic :pets, ["owners", :owner_id, "pets"] do
  description "Pets of one owner."

  message :adopted do
    description "The owner adopted a pet."
    field :pet, MyApp.Pet, description: "The adopted pet."
  end

  message :rehomed, description: "The pet moved to a new owner."
end
```

A hand-written `@moduledoc` stays first, and `@moduledoc false` keeps the manifest out of
the docs.

## Broadcast

```elixir
defmodule MyApp.Pets do
  use VerifiedPubSub, manifest: MyApp.Topics

  def adopt(attrs) do
    with {:ok, pet} <- insert(attrs) do
      broadcast!(:pets, %{owner_id: pet.owner_id}, :adopted, %{
        id: pet.id,
        name: pet.name
      })

      {:ok, pet}
    end
  end
end
```

Anyone can give a treat to anyone's pet. The broadcast goes to the pet's owner:

```elixir
broadcast!(:treats, %{owner_id: pet.owner_id}, :given, %{
  pet_id: pet.id,
  from_owner_id: giver.id,
  treat: :biscuit
})
```

To skip the sender, which a LiveView that both writes and subscribes usually wants:

```elixir
broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, payload)
```

## Subscribe

```elixir
defmodule MyAppWeb.PetsLive do
  use MyAppWeb, :live_view
  use VerifiedPubSub.Subscriber, manifest: MyApp.Topics

  def mount(_params, _session, socket) do
    if connected?(socket) do
      :ok = subscribe(:pets, %{owner_id: socket.assigns.owner.id})
    end

    {:ok, stream(socket, :pets, [])}
  end

  handle_message :pets, :adopted, payload, socket do
    {:noreply, stream_insert(socket, :pets, payload)}
  end

  ignore_message :pets, :rehomed
end
```

Every event on a subscribed topic must be handled or explicitly ignored. To match on the
topic params, put them where a broadcast takes them:

```elixir
handle_message :pets, %{owner_id: owner}, :adopted, payload, socket do
  {:noreply, assign(socket, :owner_id, owner)}
end
```

## Compile-time errors

A typo'd event:

```elixir
broadcast!(:pets, %{owner_id: id}, :adoptd, %{id: "p1", name: "Biscuit"})
```

```
** (CompileError) lib/my_app_web/live/pet_live.ex:42: unknown event :adoptd on topic :pets (owners:<owner_id>:pets).

Declared events: [:adopted, :rehomed]
:pets is declared at lib/my_app/topics.ex:7.
```

Errors that a manifest change could fix point at the declaration. A manifest error is
also reported as a diagnostic on the offending line, so an editor can mark it.

A subscriber that does not account for every event:

```elixir
handle_message :pets, :adopted, payload, socket do
  {:noreply, socket}
end
```

```
** (CompileError) MyAppWeb.PetsLive subscribes to topics with events it does not
account for:

  * :pets, :rehomed

Add a `handle_message` clause for each, or dismiss it explicitly:

    ignore_message :pets, :rehomed

Manifest: MyApp.Topics
```

Unknown topics, mistyped params, and literal payloads that don't match the declared
fields all fail the same way.

## Runtime errors

A payload built at runtime is checked on every broadcast, in every environment:

```elixir
broadcast!(:pets, %{owner_id: id}, :adopted, %{id: 42, extra: true})
```

```
** (VerifiedPubSub.PayloadError) invalid payload for :pets :adopted in MyApp.Topics:

  * missing required key: :name
  * unexpected key: :extra — not declared on this event
  * :id is declared as :string, got: 42
```

## Using with LLMs

VerifiedPubSub ships a `usage-rules.md` for coding agents. With
[usage_rules](https://hex.pm/packages/usage_rules) installed, add it to `project/0` in your
`mix.exs`:

```elixir
usage_rules: [
  file: "AGENTS.md",
  usage_rules: [:verified_pubsub]
]
```

Then run `mix usage_rules.sync` to copy the rules into that file.

## Documentation

`mix docs` builds the full documentation locally.
