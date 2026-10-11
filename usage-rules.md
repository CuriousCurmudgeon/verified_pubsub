# Rules for working with VerifiedPubSub

VerifiedPubSub wraps `Phoenix.PubSub` with a manifest of topics and events, so typos and
unhandled events become compile errors. A project can adopt it one topic at a time, so
raw `Phoenix.PubSub` code for other topics may sit alongside it.

## Never bypass the manifest for a declared topic

For a topic declared in a manifest, the checks only work if every broadcast and
subscription goes through the manifest:

- Do not call `Phoenix.PubSub.broadcast/3`, `subscribe/2` or friends on it directly, and do
  not build its topic string by hand. Use the macros below.
- Do not hand-write `handle_info/2` clauses that match `%VerifiedPubSub.Message{}`. Use
  `handle_message`.
- Do not send it a tuple like `{:created, campaign}`. Broadcast a declared event with a
  payload map.

Leave raw `Phoenix.PubSub` code for undeclared topics as it is unless asked to migrate it.
Calling `Phoenix.PubSub` directly is also the way to broadcast when the topic or event is
only known at runtime.

## Declare topics in the manifest

```elixir
defmodule MyApp.Topics do
  use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

  topic :campaigns, ["accounts", :account_id, "campaigns"] do
    message :created do
      field :id, :string
      field :campaign, MyApp.Campaign
      field :tags, {:list, :string}
      field :note, :string, required: false
    end

    message :deleted do
      field :id, :string
    end
  end

  topic :system, ["system"] do
    message :alert do
      field :text, :string
    end
  end
end
```

- The segment list is the wire topic, joined with `:`. Strings are literal segments, and
  atoms are params. Params are derived from the segments; never declare them separately.
- Field types: `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list`,
  `:any`, `{:list, type}`, or a struct module.
- `required: false` lets the key be absent or `nil`. Fields are required by default.
- `:pubsub` names a `Phoenix.PubSub` started in the supervision tree.

## Broadcast

```elixir
defmodule MyApp.Campaigns do
  use VerifiedPubSub, manifest: MyApp.Topics

  def notify(campaign) do
    broadcast!(:campaigns, %{account_id: campaign.account_id}, :created, %{
      id: campaign.id,
      campaign: campaign,
      tags: campaign.tags
    })
  end
end
```

- The argument order is always `topic, params, event, payload`.
- A topic without params omits the params argument: `broadcast!(:system, :alert, %{text: "hi"})`.
- Topic and event must be **literal atoms**, because these are macros. They cannot be
  piped into, captured with `&`, or called with `apply/3`. Params and payload can be built
  at runtime.
- The payload is a map of **exactly** the declared fields. Never pass a struct or an Ecto
  schema as the payload, and never add undeclared keys. Put a struct inside a field
  instead: `%{campaign: campaign}`.
- `broadcast!/4` raises on failure, and `broadcast/4` returns `:ok | {:error, term}`. Both
  raise `VerifiedPubSub.PayloadError` for an invalid payload.
- `broadcast_from!(self(), :campaigns, params, :created, payload)` skips the sender. Use it
  in a LiveView that both writes to a topic and subscribes to it. Inside a `Task`, an Oban
  job or a context function, pass the originating pid, not `self()`.
- `topic(:campaigns, %{account_id: id})` returns the wire string, if you need it.

## Subscribe and handle

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
    {:noreply, stream_insert(socket, :campaigns, payload.campaign)}
  end

  handle_message :campaigns, %{account_id: account_id}, :deleted, %{id: id}, socket do
    {:noreply, assign(socket, :last_deleted, {account_id, id})}
  end
end
```

- `use VerifiedPubSub.Subscriber` also imports the broadcast macros. Do not add
  `use VerifiedPubSub` as well.
- `handle_message` is a macro at module level, not a `def`. Use it in GenServers as well as
  LiveViews. It takes `topic, event, payload_pattern, state`, or with params
  `topic, params_pattern, event, payload_pattern, state`, the same order as `broadcast!`.
- Params go right after the topic, never in the payload pattern. A params pattern may name
  a subset of the params.
- **Every event on a topic the module handles must be handled or ignored**, or the module
  does not compile. Dismiss the ones you don't care about:
  `ignore_message :campaigns, :deleted` or `ignore_message :campaigns, [:created, :deleted]`.
- In a LiveView, subscribe only when `connected?(socket)`.
- Your own `handle_info/2` clauses still work alongside `handle_message`. They are matched
  after the generated clauses, so keep any existing clauses for topics that are not in the
  manifest.
- Defining handlers removes the default `handle_info/2`. If the process receives other
  messages, add your own catch-all `def handle_info(_msg, state), do: {:noreply, state}`
  after the handlers.
- If you match on literal param values, end with a clause that matches any value. Otherwise
  a message with a different value raises `FunctionClauseError`.
- Keep the default `on_missing: :error`. Do not set `:warn` or `:ignore` to make an error
  go away.

## Changing the manifest

Adding an event to a topic breaks compilation in every module that handles that topic.
That is intended. Fix each module the compiler names with a `handle_message` or an
`ignore_message`. Renaming or removing an event breaks every broadcast and handler that
uses it. Update them all; don't loosen the manifest.

## Migrating an existing topic

Moving a topic into the manifest changes what goes over the wire. Subscribers receive a
`%VerifiedPubSub.Message{}` instead of whatever the raw broadcast sent, so a raw subscriber
left behind stops matching without any error.

1. Search the whole project for the topic string, including interpolated forms like
   `"accounts:#{id}:campaigns"`, to find every broadcaster and subscriber.
2. Declare the topic with segments that build the same wire string. Segments are joined
   with `:`, so a topic that uses another separator, such as `"users/#{id}"`, cannot be
   declared as is.
3. Convert every broadcaster and every subscriber **in the same change**. Never convert
   only one side.
4. Remove the old `handle_info/2` clauses for that topic, and keep the clauses for topics
   that are not migrated yet.

## Reading the errors

| Error | Fix |
| --- | --- |
| `unknown topic` / `unknown event` | Use one of the names the error lists, or declare it in the manifest. |
| `wrong params for topic` | Pass exactly the params the error lists (missing or unexpected keys). |
| `invalid payload` (compile time) | Make the literal payload match the declared fields shown. |
| `... declares no :x` in a `handle_message` | The payload pattern names an undeclared key. If it is a param, move it right after the topic. |
| `subscribes to topics with events it does not account for` | Add `handle_message` or `ignore_message` for each listed event. |
| `expected a literal atom for topic/event` | Write the atom inline. Don't use a variable or module attribute. |
| `no manifest is in scope` | Add `use VerifiedPubSub, manifest: MyApp.Topics`. |
| `VerifiedPubSub.PayloadError` (runtime) | Build the payload with exactly the declared keys and types. |
| `VerifiedPubSub.TopicError` (runtime) | A param value was empty or contained `:`. |
| `KeyError` from a broadcast | A runtime params map is missing a topic param. |
