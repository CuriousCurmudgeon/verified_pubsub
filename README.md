# VerifiedPubSub

[![CI](https://github.com/CuriousCurmudgeon/verified_pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/CuriousCurmudgeon/verified_pubsub/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/verified_pubsub.svg)](https://hex.pm/packages/verified_pubsub)
[![Docs](https://img.shields.io/badge/hex-docs-purple.svg)](https://hexdocs.pm/verified_pubsub)

Compile-time verified PubSub for Elixir. It applies the idea of verified routes to topics
and events. Declare your topics and events once. Then a typo is a compile error, not a
message that no process receives.

VerifiedPubSub is before version 1.0, so its API can change between minor versions. The
[changelog](CHANGELOG.md) lists each change.

## Installation

VerifiedPubSub needs Elixir 1.16 or later. It broadcasts through a
[`Phoenix.PubSub`](https://hexdocs.pm/phoenix_pubsub) that your application starts.

With [Igniter](https://hexdocs.pm/igniter):

```sh
mix igniter.install verified_pubsub
```

The installer does these steps:

- It adds the formatter import.
- It finds the `Phoenix.PubSub` that your application starts, or adds one.
- It creates an empty `MyApp.Topics` manifest that uses that `Phoenix.PubSub`.

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

Then add the formatter import, so that the DSL has no parentheses:

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

`:pets` is the name that call sites use. The list is the wire topic. A `:` joins its
segments, so this wire topic is `"owners:<owner_id>:pets"`. A string is a literal segment,
and an atom is a param.

Each `message` declares one event on its topic. Its `field`s declare the keys that the
payload of that event must have. A payload is a map of exactly those fields. To make a
field optional, add `required: false` to it.

## Document your topics

The manifest writes its own docs. Its `@moduledoc` gets a `## Topics` section. The section
lists the wire pattern of each topic, its events, and the fields and types of each event.
So the docs always agree with the checks of the manifest. To say what a topic, an event,
or a field means, add a `description`. Put it inside the block, or use a keyword when
there is no block:

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

A `@moduledoc` that you write stays first. `@moduledoc false` keeps the manifest out of the
docs.

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

Any owner can give a treat to the pet of any owner. The broadcast goes to the owner of the
pet:

```elixir
broadcast!(:treats, %{owner_id: pet.owner_id}, :given, %{
  pet_id: pet.id,
  from_owner_id: giver.id,
  treat: :biscuit
})
```

A LiveView that writes to a topic and also subscribes to it usually wants to skip the
sender:

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

The module must handle or ignore each event on a subscribed topic. To match on the params
of the topic, put them where a broadcast puts them:

```elixir
handle_message :pets, %{owner_id: owner}, :adopted, payload, socket do
  {:noreply, assign(socket, :owner_id, owner)}
end
```

## Compile-time errors

An event with a typo:

```elixir
broadcast!(:pets, %{owner_id: id}, :adoptd, %{id: "p1", name: "Biscuit"})
```

```
** (CompileError) lib/my_app_web/live/pets_live.ex:42: unknown event :adoptd on topic :pets (owners:<owner_id>:pets).

Declared events: [:adopted, :rehomed]
:pets is declared at lib/my_app/topics.ex:7.
```

When a change to the manifest can fix an error, the error points at the declaration. A
manifest error is also a diagnostic on the line that causes it, so an editor can mark it.

A subscriber that does not handle or ignore each event:

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

These errors also stop the compile:

- an unknown topic
- params with a wrong key
- a literal payload that does not match the declared fields

## Runtime errors

Each broadcast checks a payload built at runtime, in each environment:

```elixir
broadcast!(:pets, %{owner_id: id}, :adopted, %{id: 42, extra: true})
```

```
** (VerifiedPubSub.PayloadError) invalid payload for :pets :adopted in MyApp.Topics:

  * missing required key: :name
  * unexpected key: :extra — not declared on this event
  * :id is declared as :string, got: 42
```

## Use with LLMs

VerifiedPubSub includes a `usage-rules.md` for coding agents. When you have
[usage_rules](https://hex.pm/packages/usage_rules), add it to `project/0` in your
`mix.exs`:

```elixir
usage_rules: [
  file: "AGENTS.md",
  usage_rules: [:verified_pubsub]
]
```

Then run `mix usage_rules.sync` to copy the rules into that file.

## Documentation

The full documentation is on [HexDocs](https://hexdocs.pm/verified_pubsub).

## Contributing

Please open an issue before you send a pull request. See
[CONTRIBUTING.md](https://github.com/CuriousCurmudgeon/verified_pubsub/blob/main/CONTRIBUTING.md).

## License

MIT. See [LICENSE](https://github.com/CuriousCurmudgeon/verified_pubsub/blob/main/LICENSE).
