# Rules for VerifiedPubSub

VerifiedPubSub puts a manifest of topics and events over `Phoenix.PubSub`. Then a typo or
an unhandled event is a compile error. A project can move to it one topic at a time, so
raw `Phoenix.PubSub` code for other topics can stay next to it.

## Never bypass the manifest for a declared topic

The checks work only when each broadcast and subscription goes through the manifest. For
a topic that a manifest declares:

- Do not call `Phoenix.PubSub.broadcast/3`, `subscribe/2` or a similar function on it
  directly. Do not build its wire topic by hand. Use the macros below.
- Do not write `handle_info/2` clauses that match `%VerifiedPubSub.Message{}`. Use
  `handle_message`.
- Do not send it a tuple like `{:adopted, pet}`. Broadcast a declared event with a payload
  map.

Leave raw `Phoenix.PubSub` code for undeclared topics as it is, unless the user asks you to
move it. To broadcast when the topic or the event is known only at runtime, call
`Phoenix.PubSub` directly.

## Declare topics in the manifest

```elixir
defmodule MyApp.Topics do
  use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

  topic :pets, ["owners", :owner_id, "pets"] do
    message :adopted do
      field :id, :string
      field :pet, MyApp.Pet
      field :species, :atom
      field :tricks, {:list, :string}
      field :nickname, :string, required: false
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

  topic :system, ["system"] do
    message :alert do
      field :text, :string
    end
  end
end
```

- The segment list is the wire topic. A `:` joins its segments. A string is a literal
  segment, and an atom is a param. The params come from the segments. Never declare them
  separately.
- Field types: `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list`,
  `:any`, `{:list, type}`, or a struct module.
- With `required: false`, the key can be absent or `nil`. A field is required by default.
- `:pubsub` names a `Phoenix.PubSub` that the supervision tree starts.
- Document a topic, an event, or a field with `description`. Put it inside the block, as
  `description "..."`. When there is no block, use a keyword, as
  `message :rehomed, description: "..."`. Keyword options and a `do` block together do not
  compile.
- The manifest adds a `## Topics` section to its `@moduledoc`. The section lists each
  topic, event, and field. Do not list topics or events by hand in the `@moduledoc`.

## Broadcast

```elixir
defmodule MyApp.Pets do
  use VerifiedPubSub, manifest: MyApp.Topics

  def notify_adopted(pet) do
    broadcast!(:pets, %{owner_id: pet.owner_id}, :adopted, %{
      id: pet.id,
      pet: pet,
      species: pet.species,
      tricks: pet.tricks
    })
  end

  # Anyone can give a treat to anyone's pet. The broadcast goes to the pet's owner.
  def give_treat(pet, giver, treat) do
    broadcast!(:treats, %{owner_id: pet.owner_id}, :given, %{
      pet_id: pet.id,
      from_owner_id: giver.id,
      treat: treat
    })
  end
end
```

- The argument order is always `topic, params, event, payload`.
- A topic without params takes no params argument: `broadcast!(:system, :alert, %{text: "hi"})`.
- These are macros, so the topic and the event must be **literal atoms**. A pipe cannot
  pass a value into them, `&` cannot capture them, and `apply/3` cannot call them. A call
  site can build the params and the payload at runtime.
- The payload is a map of **exactly** the declared fields. Never pass a struct or an Ecto
  schema as the payload. Never add undeclared keys. Put a struct inside a field instead:
  `%{pet: pet}`.
- `broadcast!/4` raises on failure, and `broadcast/4` returns `:ok | {:error, term}`. Both
  raise `VerifiedPubSub.PayloadError` for a payload that is not valid.
- `broadcast_from!(self(), :pets, params, :adopted, payload)` skips the sender. Use it in a
  LiveView that writes to a topic and also subscribes to it. In a `Task`, an Oban job, or
  a context function, pass the pid of the process that started the request, not `self()`.
- `topic(:pets, %{owner_id: id})` returns the wire topic, when you need it.

## Subscribe and handle

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
    {:noreply, stream_insert(socket, :pets, payload.pet)}
  end

  handle_message :pets, %{owner_id: owner_id}, :rehomed, %{id: id}, socket do
    {:noreply, assign(socket, :last_rehomed, {owner_id, id})}
  end
end
```

- `use VerifiedPubSub.Subscriber` also imports the broadcast macros. Do not also add
  `use VerifiedPubSub`.
- `handle_message` is a macro at module level, not a `def`. Use it in GenServers and in
  LiveViews. It takes `topic, event, payload_pattern, state`. With params, it takes
  `topic, params_pattern, event, payload_pattern, state`, the same order as `broadcast!`.
- Put the params right after the topic, never in the payload pattern. A params pattern can
  name a subset of the params.
- **The module must handle or ignore each event on each topic that it handles**, or the
  module does not compile. Ignore the events that the module does not need:
  `ignore_message :pets, :rehomed` or `ignore_message :pets, [:adopted, :rehomed]`.
- In a LiveView, subscribe only when `connected?(socket)`.
- Your own `handle_info/2` clauses still work next to `handle_message`. They match after
  the generated clauses. So keep the existing clauses for topics that are not in the
  manifest.
- A module with handlers loses the default `handle_info/2`. When the process receives other
  messages, add your own catch-all `def handle_info(_msg, state), do: {:noreply, state}`
  after the handlers.
- When you match on literal param values, end with a clause that matches any value.
  Otherwise a message with a different value raises `FunctionClauseError`.

## Change the manifest

A new event on a topic stops the compile of each module that handles that topic. This is
intended. Fix each module that the compiler names with a `handle_message` or an
`ignore_message`. When you rename or remove an event, each broadcast and handler that uses
it stops the compile. Update all of them. Do not make the manifest less strict.

## Move an existing topic into the manifest

A topic in the manifest sends a different message over the wire. Subscribers receive a
`%VerifiedPubSub.Message{}`, not the term that the raw broadcast sent. So a raw subscriber
that you do not convert stops matching, with no error.

1. Search the whole project for the wire topic, including interpolated forms like
   `"owners:#{id}:pets"`. This finds each broadcaster and subscriber.
2. Declare the topic with segments that build the same wire topic. A `:` joins the
   segments. So you cannot declare a topic that uses another separator, such as
   `"pets/#{id}"`, as it is.
3. Convert each broadcaster and each subscriber **in the same change**. Never convert only
   one side.
4. Remove the old `handle_info/2` clauses for that topic. Keep the clauses for topics that
   you did not move yet.

## Fix the errors

| Error | Fix |
| --- | --- |
| `unknown topic` / `unknown event` | Use one of the names that the error lists, or declare it in the manifest. |
| `wrong params for topic` | Pass exactly the params that the error lists. It shows the missing or unexpected keys. |
| `invalid payload` (compile time) | Make the literal payload match the declared fields that the error shows. |
| `... declares no :x` in a `handle_message` | The payload pattern names an undeclared key. When it is a param, move it right after the topic. |
| `subscribes to topics with events it does not account for` | Add `handle_message` or `ignore_message` for each event in the list. |
| `expected a literal atom for topic/event` | Write the atom inline. Do not use a variable or a module attribute. |
| `no manifest is in scope` | Add `use VerifiedPubSub, manifest: MyApp.Topics`. |
| `VerifiedPubSub.PayloadError` (runtime) | Build the payload with exactly the declared keys and types. |
| `VerifiedPubSub.TopicError` (runtime) | A param value was empty or had a `:` in it. |
| `KeyError` from a broadcast | A params map built at runtime does not have a param of the topic. |
