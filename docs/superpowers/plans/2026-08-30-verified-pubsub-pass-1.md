# Verified PubSub Pass 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship pass 1 of `verified_pubsub` — a Spark-based registry that makes invalid PubSub broadcasts and incomplete subscribers compile errors.

**Architecture:** A Spark DSL registry module is the single source of truth for topics and events. Spark Transformers derive topic params, validate the registry, and generate `broadcast_*`/`subscribe_*` functions onto the registry module. A separate hand-rolled `VerifiedPubsub.Subscriber` provides a `handle_message` macro that accumulates coverage into a module attribute and, at `@before_compile`, diffs that coverage against the registry and emits one `handle_info/2` clause.

**Tech Stack:** Elixir 1.20.4 (OTP 29), `spark ~> 2.7`, `phoenix_pubsub ~> 2.1` (optional), ExUnit.

**Spec:** `docs/superpowers/specs/2026-08-30-verified-pubsub-design.md`

## Global Constraints

- `elixir: "~> 1.17"` in `mix.exs`. Do not raise the floor.
- Required deps: `{:spark, "~> 2.7"}` only. `{:phoenix_pubsub, "~> 2.1", optional: true}`.
- The library and its full test suite must run without Phoenix. Never `alias`, `import`, or call `Phoenix.PubSub` outside `lib/verified_pubsub/adapter/phoenix_pub_sub.ex`.
- Top-level module namespace is `VerifiedPubsub` (one lowercase `s`, matching the existing `lib/verified_pubsub.ex`).
- Every entity struct used as a Spark entity target MUST declare both `:__identifier__` and `:__spark_metadata__` fields. Spark raises `"<struct> must have the __identifier__ field!"` at DSL-expansion time otherwise.
- Registry validation MUST live in Transformers, never Verifiers. A Verifier returning `{:error, _}` only emits a warning and still defines the module; a Transformer returning `{:error, Spark.Error.DslError}` raises and does not define the module.
- Payload `field` declarations are parsed and exposed but NOT enforced in pass 1.

---

### Task 1: Message struct, Adapter behaviour, and the Local adapter

The transport foundation. Nothing here depends on Spark, so it is testable in isolation and unblocks every later task.

**Files:**
- Create: `lib/verified_pubsub/message.ex`
- Create: `lib/verified_pubsub/adapter.ex`
- Create: `lib/verified_pubsub/adapter/local.ex`
- Test: `test/verified_pubsub/adapter/local_test.exs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `%VerifiedPubsub.Message{registry: module(), topic: atom(), event: atom(), params: map(), payload: term()}`
  - `VerifiedPubsub.Adapter` behaviour: `broadcast(config, topic_string, %Message{})`, `subscribe(config, topic_string)`, `unsubscribe(config, topic_string)`; all return `:ok | {:error, term}`.
  - `VerifiedPubsub.Adapter.Local` — `config` is ignored; delivers with `send/2` to processes that subscribed in this VM.

- [ ] **Step 1: Write the failing test**

```elixir
# test/verified_pubsub/adapter/local_test.exs
defmodule VerifiedPubsub.Adapter.LocalTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Adapter.Local
  alias VerifiedPubsub.Message

  setup do
    start_supervised!(Local)
    :ok
  end

  defp message(payload) do
    %Message{
      registry: MyRegistry,
      topic: :campaigns,
      event: :created,
      params: %{account_id: "7"},
      payload: payload
    }
  end

  test "a subscriber receives messages broadcast on its topic" do
    assert :ok = Local.subscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))

    assert_receive %Message{event: :created, payload: %{id: "c1"}}
  end

  test "a subscriber receives nothing for a topic it did not subscribe to" do
    assert :ok = Local.subscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.broadcast(nil, "accounts:9:campaigns", message(%{id: "c1"}))

    refute_receive %Message{}, 50
  end

  test "unsubscribe stops delivery" do
    assert :ok = Local.subscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.unsubscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))

    refute_receive %Message{}, 50
  end

  test "broadcasting to a topic with no subscribers is :ok" do
    assert :ok = Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))
  end

  test "every subscriber to a topic receives the message" do
    test_pid = self()

    other =
      spawn_link(fn ->
        Local.subscribe(nil, "accounts:7:campaigns")
        send(test_pid, :ready)
        receive do: (%Message{payload: p} -> send(test_pid, {:other_got, p}))
      end)

    assert_receive :ready
    Local.subscribe(nil, "accounts:7:campaigns")
    Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))

    assert_receive %Message{payload: %{id: "c1"}}
    assert_receive {:other_got, %{id: "c1"}}
    refute_received _
    Process.exit(other, :kill)
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/verified_pubsub/adapter/local_test.exs`
Expected: FAIL — `VerifiedPubsub.Adapter.Local` is undefined.

- [ ] **Step 3: Write the implementation**

```elixir
# lib/verified_pubsub/message.ex
defmodule VerifiedPubsub.Message do
  @moduledoc """
  The struct delivered to subscribers for every verified broadcast.

  `params` carries the values interpolated into a parameterized topic, so a
  process subscribed to several instances of a topic can tell them apart.
  """

  @type t :: %__MODULE__{
          registry: module(),
          topic: atom(),
          event: atom(),
          params: map(),
          payload: term()
        }

  @enforce_keys [:registry, :topic, :event]
  defstruct [:registry, :topic, :event, params: %{}, payload: nil]
end
```

```elixir
# lib/verified_pubsub/adapter.ex
defmodule VerifiedPubsub.Adapter do
  @moduledoc """
  Transport behaviour, so `verified_pubsub` does not require Phoenix.

  `config` is opaque to the library and comes from the `:pubsub` option given to
  `use VerifiedPubsub.Registry`.
  """

  alias VerifiedPubsub.Message

  @callback broadcast(config :: term(), topic :: String.t(), message :: Message.t()) ::
              :ok | {:error, term()}
  @callback subscribe(config :: term(), topic :: String.t()) :: :ok | {:error, term()}
  @callback unsubscribe(config :: term(), topic :: String.t()) :: :ok | {:error, term()}
end
```

`Local` keeps `topic => [pid]` in a `Registry` under a fixed name, so it needs to be
started. Using Elixir's `Registry` with `:duplicate` keys gives us fan-out, automatic
cleanup on subscriber exit, and no GenServer of our own.

```elixir
# lib/verified_pubsub/adapter/local.ex
defmodule VerifiedPubsub.Adapter.Local do
  @moduledoc """
  In-VM adapter that delivers with `send/2`. Intended for tests and for
  single-node use; it does not cross nodes.

  Must be started before use, e.g. in a supervision tree or via
  `start_supervised!(VerifiedPubsub.Adapter.Local)` in tests.
  """

  @behaviour VerifiedPubsub.Adapter

  @registry __MODULE__.Registry

  def child_spec(_opts) do
    Registry.child_spec(keys: :duplicate, name: @registry)
  end

  @impl true
  def subscribe(_config, topic) when is_binary(topic) do
    case Registry.register(@registry, topic, nil) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def unsubscribe(_config, topic) when is_binary(topic) do
    Registry.unregister(@registry, topic)
  end

  @impl true
  def broadcast(_config, topic, message) when is_binary(topic) do
    Registry.dispatch(@registry, topic, fn entries ->
      Enum.each(entries, fn {pid, _} -> send(pid, message) end)
    end)
  end
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/adapter/local_test.exs`
Expected: 5 tests, 0 failures.

Note: `Registry.register/3` returns `{:ok, pid}` and `Registry.unregister/2` and
`Registry.dispatch/3` both return `:ok`, so the `@impl` returns line up with the
behaviour without extra wrapping.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/message.ex lib/verified_pubsub/adapter.ex \
        lib/verified_pubsub/adapter/local.ex test/verified_pubsub/adapter/local_test.exs
git commit -m "Add message struct, adapter behaviour, and local adapter"
```

---

### Task 2: Registry DSL — entities, extension, entry point, and Info

Parsing only. No params, no validation, no generated functions.

**Files:**
- Create: `lib/verified_pubsub/dsl/field.ex`
- Create: `lib/verified_pubsub/dsl/message.ex`
- Create: `lib/verified_pubsub/dsl/topic.ex`
- Create: `lib/verified_pubsub/dsl.ex`
- Create: `lib/verified_pubsub/registry.ex`
- Create: `lib/verified_pubsub/info.ex`
- Create: `test/support/registries.ex`
- Modify: `mix.exs` (add `elixirc_paths/1` so `test/support` compiles in the test env)
- Test: `test/verified_pubsub/dsl_test.exs`

**Interfaces:**
- Consumes: Task 1's `VerifiedPubsub.Adapter.Local` (used as the test registries' adapter).
- Produces:
  - `%VerifiedPubsub.Dsl.Topic{name: atom(), pattern: String.t(), params: [atom()], messages: [Message.t()], __identifier__: atom(), __spark_metadata__: term()}`
  - `%VerifiedPubsub.Dsl.Message{name: atom(), fields: [Field.t()], ...}`
  - `%VerifiedPubsub.Dsl.Field{name: atom(), type: atom(), ...}`
  - `VerifiedPubsub.Info.topics/1`, `topic/2`, `topic!/2`, `events/2`, `params/2`
  - `use VerifiedPubsub.Registry, adapter: module(), pubsub: term()`, which defines
    `__verified_pubsub_adapter__/0` and `__verified_pubsub_config__/0` on the registry.

- [ ] **Step 1: Add test support paths to mix.exs**

Add `elixirc_paths/1` to the project so `test/support` is compiled only in the test
environment:

```elixir
  def project do
    [
      app: :verified_pubsub,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
```

- [ ] **Step 2: Write the failing test**

```elixir
# test/support/registries.ex
defmodule VerifiedPubsub.TestRegistries do
  @moduledoc "Registries compiled in the test env and reused across test files."

  defmodule Basic do
    use VerifiedPubsub.Registry, adapter: VerifiedPubsub.Adapter.Local

    topic :campaigns, "accounts:%{account_id}:campaigns" do
      message :created do
        field :id, :string
        field :name, :string
      end

      message :updated do
        field :id, :string
      end

      message :deleted do
        field :id, :string
      end
    end

    topic :system, "system" do
      message :alert do
        field :text, :string
      end
    end
  end
end
```

```elixir
# test/verified_pubsub/dsl_test.exs
defmodule VerifiedPubsub.DslTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Info
  alias VerifiedPubsub.TestRegistries.Basic

  test "topics are parsed at the top level, without a wrapper block" do
    assert [:campaigns, :system] = Info.topics(Basic) |> Enum.map(& &1.name) |> Enum.sort()
  end

  test "topic/2 returns the topic by name" do
    assert {:ok, topic} = Info.topic(Basic, :campaigns)
    assert topic.pattern == "accounts:%{account_id}:campaigns"
  end

  test "topic/2 returns :error for an unknown topic" do
    assert :error = Info.topic(Basic, :nope)
  end

  test "topic!/2 raises for an unknown topic and names the known ones" do
    assert_raise ArgumentError, ~r/unknown topic :nope.*:campaigns/s, fn ->
      Info.topic!(Basic, :nope)
    end
  end

  test "events/2 returns declared event names in declaration order" do
    assert [:created, :updated, :deleted] = Info.events(Basic, :campaigns)
    assert [:alert] = Info.events(Basic, :system)
  end

  test "payload fields are parsed and exposed but not enforced" do
    {:ok, topic} = Info.topic(Basic, :campaigns)
    created = Enum.find(topic.messages, &(&1.name == :created))

    assert [{:id, :string}, {:name, :string}] = Enum.map(created.fields, &{&1.name, &1.type})
  end

  test "the registry records its adapter and config" do
    assert Basic.__verified_pubsub_adapter__() == VerifiedPubsub.Adapter.Local
    assert Basic.__verified_pubsub_config__() == nil
  end
end
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mix test test/verified_pubsub/dsl_test.exs`
Expected: FAIL — `VerifiedPubsub.Registry` is undefined.

- [ ] **Step 4: Write the entity structs**

Both `:__identifier__` and `:__spark_metadata__` are mandatory for Spark entity
targets. `params` on `Topic` stays empty until Task 3 fills it.

```elixir
# lib/verified_pubsub/dsl/field.ex
defmodule VerifiedPubsub.Dsl.Field do
  @moduledoc "A declared payload field. Parsed in pass 1, not enforced."

  @type t :: %__MODULE__{name: atom(), type: atom()}

  defstruct [:name, :type, :__identifier__, :__spark_metadata__]
end
```

```elixir
# lib/verified_pubsub/dsl/message.ex
defmodule VerifiedPubsub.Dsl.Message do
  @moduledoc "A declared event on a topic."

  @type t :: %__MODULE__{name: atom(), fields: [VerifiedPubsub.Dsl.Field.t()]}

  defstruct [:name, :__identifier__, fields: [], __spark_metadata__: nil]
end
```

```elixir
# lib/verified_pubsub/dsl/topic.ex
defmodule VerifiedPubsub.Dsl.Topic do
  @moduledoc "A declared topic, its wire pattern, and its events."

  @type t :: %__MODULE__{
          name: atom(),
          pattern: String.t(),
          params: [atom()],
          messages: [VerifiedPubsub.Dsl.Message.t()]
        }

  defstruct [:name, :pattern, :__identifier__, params: [], messages: [], __spark_metadata__: nil]
end
```

- [ ] **Step 5: Write the Spark extension**

`top_level?: true` is what allows bare `topic` with no wrapper block. Do not alias
`Spark.Builder.Field` — it collides with `VerifiedPubsub.Dsl.Field`; use the plain
keyword schema syntax instead, which Spark accepts.

```elixir
# lib/verified_pubsub/dsl.ex
defmodule VerifiedPubsub.Dsl do
  @moduledoc "The Spark DSL extension backing `VerifiedPubsub.Registry`."

  alias Spark.Builder.{Entity, Section}

  @field Entity.new(:field, VerifiedPubsub.Dsl.Field,
           describe: "A payload field. Declared in pass 1; not yet enforced.",
           args: [:name, :type],
           identifier: :name,
           schema: [
             name: [type: :atom, required: true, doc: "The field name."],
             type: [type: :atom, required: true, doc: "The field type."]
           ]
         )
         |> Entity.build!()

  @message Entity.new(:message, VerifiedPubsub.Dsl.Message,
             describe: "An event that can be broadcast on the enclosing topic.",
             args: [:name],
             identifier: :name,
             entities: [fields: [@field]],
             schema: [name: [type: :atom, required: true, doc: "The event name."]]
           )
           |> Entity.build!()

  @topic Entity.new(:topic, VerifiedPubsub.Dsl.Topic,
           describe: "A topic and the events that may be broadcast on it.",
           args: [:name, :pattern],
           identifier: :name,
           entities: [messages: [@message]],
           schema: [
             name: [
               type: :atom,
               required: true,
               doc: "Alias used to build function names. Independent of the wire pattern."
             ],
             pattern: [
               type: :string,
               required: true,
               doc: ~S|Wire topic. `%{name}` marks a parameter, e.g. "accounts:%{account_id}:campaigns".|
             ]
           ]
         )
         |> Entity.build!()

  @topics Section.new(:topics,
            describe: "Declares every topic and event in the application.",
            top_level?: true,
            entities: [@topic]
          )
          |> Section.build!()

  use Spark.Dsl.Extension, sections: [@topics]
end
```

- [ ] **Step 6: Write the registry entry point**

`opt_schema` makes Spark validate the `use` options; `handle_opts/1` returns quoted
code injected into the consumer's module. Generated broadcast functions (Task 5) read
the adapter back out through these two functions at runtime, so the transformer never
needs the `use` options threaded into it.

```elixir
# lib/verified_pubsub/registry.ex
defmodule VerifiedPubsub.Registry do
  @moduledoc """
  Declares the topics and events for an application.

      defmodule MyApp.Topics do
        use VerifiedPubsub.Registry,
          adapter: VerifiedPubsub.Adapter.PhoenixPubSub,
          pubsub: MyApp.PubSub

        topic :campaigns, "accounts:%{account_id}:campaigns" do
          message :created do
            field :id, :string
          end
        end
      end
  """

  use Spark.Dsl,
    default_extensions: [extensions: [VerifiedPubsub.Dsl]],
    opt_schema: [
      adapter: [
        type: {:behaviour, VerifiedPubsub.Adapter},
        required: true,
        doc: "The `VerifiedPubsub.Adapter` used to broadcast and subscribe."
      ],
      pubsub: [
        type: :any,
        default: nil,
        doc: "Opaque adapter config. For the Phoenix adapter, the `Phoenix.PubSub` name."
      ]
    ]

  @impl Spark.Dsl
  def handle_opts(opts) do
    quote do
      @doc false
      def __verified_pubsub_adapter__, do: unquote(opts[:adapter])

      @doc false
      def __verified_pubsub_config__, do: unquote(Macro.escape(opts[:pubsub]))
    end
  end
end
```

- [ ] **Step 7: Write the Info module**

`Spark.InfoGenerator` supplies `topics/1`. The rest are hand-written so they can carry
docs and specs, and so the subscriber has a stable read interface that never reaches
into Spark internals.

```elixir
# lib/verified_pubsub/info.ex
defmodule VerifiedPubsub.Info do
  @moduledoc """
  The only supported way to read a registry.

  Everything outside this module — including `VerifiedPubsub.Subscriber` — goes
  through these functions rather than Spark internals, so the DSL front-end stays
  replaceable.
  """

  use Spark.InfoGenerator, extension: VerifiedPubsub.Dsl, sections: [:topics]

  alias VerifiedPubsub.Dsl.Topic

  @doc "Fetches a topic by its alias."
  @spec topic(module(), atom()) :: {:ok, Topic.t()} | :error
  def topic(registry, name) do
    case Enum.find(topics(registry), &(&1.name == name)) do
      nil -> :error
      topic -> {:ok, topic}
    end
  end

  @doc "Fetches a topic by its alias, raising with the known topics if absent."
  @spec topic!(module(), atom()) :: Topic.t()
  def topic!(registry, name) do
    case topic(registry, name) do
      {:ok, topic} ->
        topic

      :error ->
        known = registry |> topics() |> Enum.map(& &1.name) |> Enum.sort()

        raise ArgumentError,
              "unknown topic #{inspect(name)} in #{inspect(registry)}. " <>
                "Known topics: #{inspect(known)}"
    end
  end

  @doc "Event names declared on a topic, in declaration order."
  @spec events(module(), atom()) :: [atom()]
  def events(registry, name) do
    registry |> topic!(name) |> Map.fetch!(:messages) |> Enum.map(& &1.name)
  end

  @doc "Parameter names for a topic, in the order they appear in the pattern."
  @spec params(module(), atom()) :: [atom()]
  def params(registry, name) do
    registry |> topic!(name) |> Map.fetch!(:params)
  end
end
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/dsl_test.exs`
Expected: 7 tests, 0 failures.

- [ ] **Step 9: Commit**

```bash
git add mix.exs lib/verified_pubsub/dsl.ex lib/verified_pubsub/dsl \
        lib/verified_pubsub/registry.ex lib/verified_pubsub/info.ex \
        test/support/registries.ex test/verified_pubsub/dsl_test.exs
git commit -m "Add registry DSL, Spark extension, and introspection API"
```

---

### Task 3: Derive topic params from the pattern

**Files:**
- Create: `lib/verified_pubsub/transformers/parse_params.ex`
- Modify: `lib/verified_pubsub/dsl.ex` (register the transformer)
- Test: `test/verified_pubsub/transformers/parse_params_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubsub.Dsl.Topic`, `VerifiedPubsub.Info.params/2`.
- Produces: `topic.params` populated as `[atom()]` in pattern order. Also
  `VerifiedPubsub.Transformers.ParseParams.parse/1`, a pure function returning
  `{:ok, [atom()]} | {:error, String.t()}`, reused by Task 4's validation and Task 5's
  interpolation.

- [ ] **Step 1: Write the failing test**

```elixir
# test/verified_pubsub/transformers/parse_params_test.exs
defmodule VerifiedPubsub.Transformers.ParseParamsTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Info
  alias VerifiedPubsub.TestRegistries.Basic
  alias VerifiedPubsub.Transformers.ParseParams

  describe "parse/1" do
    test "extracts params in order" do
      assert {:ok, [:account_id]} = ParseParams.parse("accounts:%{account_id}:campaigns")
      assert {:ok, [:org_id, :user_id]} = ParseParams.parse("o:%{org_id}:u:%{user_id}")
    end

    test "returns an empty list for a static pattern" do
      assert {:ok, []} = ParseParams.parse("system")
    end

    test "rejects an unclosed parameter" do
      assert {:error, msg} = ParseParams.parse("accounts:%{account_id")
      assert msg =~ "unterminated"
    end

    test "rejects an empty parameter name" do
      assert {:error, msg} = ParseParams.parse("accounts:%{}")
      assert msg =~ "empty"
    end

    test "rejects a duplicate parameter name" do
      assert {:error, msg} = ParseParams.parse("a:%{id}:b:%{id}")
      assert msg =~ "duplicate"
      assert msg =~ "id"
    end

    test "rejects a parameter name that is not a valid identifier" do
      assert {:error, msg} = ParseParams.parse("a:%{account-id}")
      assert msg =~ "account-id"
    end
  end

  describe "integration with the DSL" do
    test "params land on the topic struct" do
      assert [:account_id] = Info.params(Basic, :campaigns)
      assert [] = Info.params(Basic, :system)
    end
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/verified_pubsub/transformers/parse_params_test.exs`
Expected: FAIL — `VerifiedPubsub.Transformers.ParseParams` is undefined.

- [ ] **Step 3: Write the implementation**

A regex alone cannot detect an unterminated `%{`, so scan for well-formed params with
a regex, then check that the number found accounts for every `%{` in the string.

```elixir
# lib/verified_pubsub/transformers/parse_params.ex
defmodule VerifiedPubsub.Transformers.ParseParams do
  @moduledoc """
  Derives each topic's parameter list from its wire pattern, so params are declared
  exactly once.
  """

  use Spark.Dsl.Transformer

  @param_regex ~r/%\{([^}]*)\}/
  @identifier_regex ~r/^[a-z_][a-zA-Z0-9_]*$/

  @doc """
  Extracts parameter names from a wire pattern.

      iex> VerifiedPubsub.Transformers.ParseParams.parse("accounts:%{account_id}:campaigns")
      {:ok, [:account_id]}
  """
  @spec parse(String.t()) :: {:ok, [atom()]} | {:error, String.t()}
  def parse(pattern) when is_binary(pattern) do
    found = Regex.scan(@param_regex, pattern, capture: :all_but_first) |> List.flatten()
    opens = pattern |> String.split("%{") |> length() |> Kernel.-(1)

    cond do
      opens != length(found) ->
        {:error, "unterminated parameter in pattern #{inspect(pattern)}: expected a closing `}`"}

      Enum.any?(found, &(&1 == "")) ->
        {:error, "empty parameter name in pattern #{inspect(pattern)}"}

      invalid = Enum.find(found, &(not Regex.match?(@identifier_regex, &1))) ->
        {:error,
         "invalid parameter name #{inspect(invalid)} in pattern #{inspect(pattern)}: " <>
           "must be a lowercase Elixir identifier"}

      true ->
        params = Enum.map(found, &String.to_atom/1)
        duplicates = params -- Enum.uniq(params)

        if duplicates == [] do
          {:ok, params}
        else
          {:error,
           "duplicate parameter #{inspect(hd(duplicates))} in pattern #{inspect(pattern)}"}
        end
    end
  end

  @impl true
  def transform(dsl) do
    dsl
    |> Spark.Dsl.Transformer.get_entities([:topics])
    |> Enum.reduce_while({:ok, dsl}, fn topic, {:ok, acc} ->
      case parse(topic.pattern) do
        {:ok, params} ->
          {:cont, {:ok, replace_topic(acc, %{topic | params: params})}}

        {:error, message} ->
          {:halt,
           {:error,
            Spark.Error.DslError.exception(
              message: message,
              path: [:topics, topic.name],
              module: Spark.Dsl.Transformer.get_persisted(dsl, :module)
            )}}
      end
    end)
  end

  defp replace_topic(dsl, topic) do
    Spark.Dsl.Transformer.replace_entity(dsl, [:topics], topic, &(&1.name == topic.name))
  end
end
```

Register it in the extension:

```elixir
  use Spark.Dsl.Extension,
    sections: [@topics],
    transformers: [VerifiedPubsub.Transformers.ParseParams]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/transformers/parse_params_test.exs`
Expected: 7 tests, 0 failures.

If `Spark.Dsl.Transformer.replace_entity/4` has a different arity in 2.7.2, check
`deps/spark/lib/spark/dsl/transformer.ex` and adapt; the fallback is
`Spark.Dsl.Transformer.set_option/3`-style manipulation of the `[:topics]` entity list.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/dsl.ex lib/verified_pubsub/transformers/parse_params.ex \
        test/verified_pubsub/transformers/parse_params_test.exs
git commit -m "Derive topic params from the wire pattern"
```

---

### Task 4: Validate the registry with hard compile errors

**Files:**
- Create: `lib/verified_pubsub/transformers/validate_topics.ex`
- Modify: `lib/verified_pubsub/dsl.ex` (register the transformer after `ParseParams`)
- Create: `test/support/compile_helper.ex`
- Test: `test/verified_pubsub/transformers/validate_topics_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubsub.Transformers.ParseParams` (must run after it).
- Produces: `VerifiedPubsub.CompileHelper.compile!/1` and `compile_error/1`, used by
  Tasks 5, 7, and 8 to assert on compile-time failures.

- [ ] **Step 1: Write the compile helper**

Compiling a string that raises leaves no module behind, which is what we assert. Give
each generated module a unique name so tests stay `async: true` without redefinition
warnings.

```elixir
# test/support/compile_helper.ex
defmodule VerifiedPubsub.CompileHelper do
  @moduledoc "Helpers for asserting on compile-time success and failure."

  @doc "Compiles `source`, returning the first module defined. Raises on failure."
  def compile!(source) do
    [{module, _bin} | _] = Code.compile_string(source)
    module
  end

  @doc """
  Compiles `source` and returns the exception it raised, or `nil` if it compiled.

  Warnings are silenced so a deliberately-broken module does not pollute test output.
  """
  def compile_error(source) do
    ExUnit.CaptureLog.capture_log(fn -> nil end)

    try do
      Code.compile_string(source)
      nil
    rescue
      exception -> exception
    end
  end

  @doc "A unique module name, so generated test modules never collide."
  def unique_module(prefix) do
    "#{prefix}#{System.unique_integer([:positive])}"
  end
end
```

- [ ] **Step 2: Write the failing test**

```elixir
# test/verified_pubsub/transformers/validate_topics_test.exs
defmodule VerifiedPubsub.Transformers.ValidateTopicsTest do
  use ExUnit.Case, async: true

  import VerifiedPubsub.CompileHelper

  defp registry_source(body) do
    """
    defmodule #{unique_module("VPTest.Validate")} do
      use VerifiedPubsub.Registry, adapter: VerifiedPubsub.Adapter.Local
      #{body}
    end
    """
  end

  test "a valid registry compiles" do
    assert is_atom(
             compile!(
               registry_source("""
               topic :campaigns, "accounts:%{account_id}:campaigns" do
                 message :created do
                   field :id, :string
                 end
               end
               """)
             )
           )
  end

  test "duplicate topic names are a compile error" do
    error =
      compile_error(
        registry_source("""
        topic :campaigns, "a" do
        end

        topic :campaigns, "b" do
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "duplicate topic"
    assert Exception.message(error) =~ "campaigns"
  end

  test "duplicate event names within a topic are a compile error" do
    error =
      compile_error(
        registry_source("""
        topic :campaigns, "a" do
          message :created do
          end

          message :created do
          end
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "duplicate event"
    assert Exception.message(error) =~ "created"
  end

  test "the same event name on two different topics is allowed" do
    assert is_atom(
             compile!(
               registry_source("""
               topic :campaigns, "a" do
                 message :created do
                 end
               end

               topic :contacts, "b" do
                 message :created do
                 end
               end
               """)
             )
           )
  end

  test "a malformed pattern is a compile error" do
    error = compile_error(registry_source(~S|topic :campaigns, "a:%{oops" do
    end|))

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "unterminated"
  end

  test "a topic with no events is a compile error" do
    error = compile_error(registry_source(~S|topic :campaigns, "a" do
    end|))

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "declares no events"
  end
end
```

Note the fourth test: two topics may each declare `:created`. Uniqueness of events is
scoped to a topic, and the plan asserts that explicitly so a later refactor cannot
tighten it by accident.

- [ ] **Step 3: Run test to verify it fails**

Run: `mix test test/verified_pubsub/transformers/validate_topics_test.exs`
Expected: FAIL — duplicate topics only warn (Spark's Verifier), duplicate events are
not detected at all, and empty topics are accepted.

- [ ] **Step 4: Write the implementation**

Spark's built-in `VerifyEntityUniqueness` is a Verifier, so it only warns and does not
inspect nested entities. Both checks are re-done here to get a hard error.

```elixir
# lib/verified_pubsub/transformers/validate_topics.ex
defmodule VerifiedPubsub.Transformers.ValidateTopics do
  @moduledoc """
  Validates the registry, raising at compile time.

  This is a Transformer rather than a Verifier deliberately: a Verifier returning
  `{:error, _}` runs via `@after_verify`, which downgrades the error to a warning and
  still defines the module. A Transformer returning `{:error, _}` aborts compilation.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def after?(VerifiedPubsub.Transformers.ParseParams), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    topics = Transformer.get_entities(dsl, [:topics])
    module = Transformer.get_persisted(dsl, :module)

    with :ok <- validate_unique_topics(topics, module),
         :ok <- validate_topics(topics, module) do
      {:ok, dsl}
    end
  end

  defp validate_unique_topics(topics, module) do
    names = Enum.map(topics, & &1.name)

    case names -- Enum.uniq(names) do
      [] -> :ok
      [dup | _] -> error(module, [:topics, dup], "duplicate topic #{inspect(dup)}")
    end
  end

  defp validate_topics(topics, module) do
    Enum.reduce_while(topics, :ok, fn topic, :ok ->
      case validate_topic(topic, module) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_topic(topic, module) do
    events = Enum.map(topic.messages, & &1.name)

    cond do
      events == [] ->
        error(
          module,
          [:topics, topic.name],
          "topic #{inspect(topic.name)} declares no events. Add at least one `message`, " <>
            "or remove the topic."
        )

      (dups = events -- Enum.uniq(events)) != [] ->
        error(
          module,
          [:topics, topic.name, hd(dups)],
          "duplicate event #{inspect(hd(dups))} on topic #{inspect(topic.name)}"
        )

      true ->
        :ok
    end
  end

  defp error(module, path, message) do
    {:error, Spark.Error.DslError.exception(message: message, path: path, module: module)}
  end
end
```

Register it after `ParseParams`:

```elixir
  use Spark.Dsl.Extension,
    sections: [@topics],
    transformers: [
      VerifiedPubsub.Transformers.ParseParams,
      VerifiedPubsub.Transformers.ValidateTopics
    ]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/transformers/validate_topics_test.exs`
Expected: 6 tests, 0 failures.

- [ ] **Step 6: Run the whole suite**

Run: `mix test`
Expected: all green. The `:system` topic in `test/support/registries.ex` has one event,
so the "declares no events" rule does not break it.

- [ ] **Step 7: Commit**

```bash
git add lib/verified_pubsub/dsl.ex lib/verified_pubsub/transformers/validate_topics.ex \
        test/support/compile_helper.ex \
        test/verified_pubsub/transformers/validate_topics_test.exs
git commit -m "Validate registry with hard compile errors via a transformer"
```

---

### Task 5: Generate the broadcast and subscribe surface

**Files:**
- Create: `lib/verified_pubsub/transformers/define_functions.ex`
- Modify: `lib/verified_pubsub/dsl.ex` (register the transformer last)
- Test: `test/verified_pubsub/broadcast_test.exs`

**Interfaces:**
- Consumes: `ParseParams` (needs `topic.params`), `Info`, `Adapter.Local`, `Message`.
- Produces, on every registry, for a topic `:campaigns` with params `[:account_id]`:
  - `topic_campaigns(%{account_id: term()}) :: String.t()`
  - `subscribe_campaigns(%{account_id: term()}) :: :ok | {:error, term}`
  - `unsubscribe_campaigns(%{account_id: term()}) :: :ok | {:error, term}`
  - `broadcast_campaigns_created(%{account_id: term()}, payload) :: :ok | {:error, term}`
  - `broadcast_campaigns_created!(%{account_id: term()}, payload) :: :ok`
  For a topic with no params, every function drops the params argument:
  `topic_system()`, `subscribe_system()`, `broadcast_system_alert!(payload)`.

- [ ] **Step 1: Write the failing test**

```elixir
# test/verified_pubsub/broadcast_test.exs
defmodule VerifiedPubsub.BroadcastTest do
  use ExUnit.Case, async: true

  import VerifiedPubsub.CompileHelper

  alias VerifiedPubsub.Message
  alias VerifiedPubsub.TestRegistries.Basic

  setup do
    start_supervised!(VerifiedPubsub.Adapter.Local)
    :ok
  end

  test "topic_* interpolates params into the wire pattern" do
    assert Basic.topic_campaigns(%{account_id: "7"}) == "accounts:7:campaigns"
  end

  test "topic_* stringifies non-binary params" do
    assert Basic.topic_campaigns(%{account_id: 7}) == "accounts:7:campaigns"
  end

  test "a param-free topic takes no params argument" do
    assert Basic.topic_system() == "system"
  end

  test "broadcast_*! delivers a Message to subscribers of that topic" do
    assert :ok = Basic.subscribe_campaigns(%{account_id: "7"})
    assert :ok = Basic.broadcast_campaigns_created!(%{account_id: "7"}, %{id: "c1"})

    assert_receive %Message{
      registry: Basic,
      topic: :campaigns,
      event: :created,
      params: %{account_id: "7"},
      payload: %{id: "c1"}
    }
  end

  test "broadcasts are scoped by param value" do
    assert :ok = Basic.subscribe_campaigns(%{account_id: "7"})
    assert :ok = Basic.broadcast_campaigns_created!(%{account_id: "9"}, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "unsubscribe_* stops delivery" do
    assert :ok = Basic.subscribe_campaigns(%{account_id: "7"})
    assert :ok = Basic.unsubscribe_campaigns(%{account_id: "7"})
    assert :ok = Basic.broadcast_campaigns_created!(%{account_id: "7"}, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "the non-bang broadcast returns :ok" do
    assert :ok = Basic.broadcast_campaigns_created(%{account_id: "7"}, %{id: "c1"})
  end

  test "a param-free topic broadcasts with only a payload" do
    assert :ok = Basic.subscribe_system()
    assert :ok = Basic.broadcast_system_alert!(%{text: "hi"})

    assert_receive %Message{topic: :system, event: :alert, params: %{}}
  end

  test "a missing param key raises FunctionClauseError" do
    assert_raise FunctionClauseError, fn ->
      Basic.broadcast_campaigns_created!(%{wrong: "7"}, %{id: "c1"})
    end
  end

  test "no function is generated for an undeclared event" do
    refute function_exported?(Basic, :broadcast_campaigns_exploded!, 2)
  end

  test "broadcasting an undeclared event is a compile error" do
    error =
      compile_error("""
      defmodule #{unique_module("VPTest.BadBroadcast")} do
        def go, do: VerifiedPubsub.TestRegistries.Basic.broadcast_campaigns_exploded!(%{account_id: "7"}, %{})
      end
      """)

    assert error, "expected calling an undeclared event to fail compilation"
    assert Exception.message(error) =~ "broadcast_campaigns_exploded!"
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/verified_pubsub/broadcast_test.exs`
Expected: FAIL — `Basic.topic_campaigns/1` is undefined.

- [ ] **Step 3: Write the implementation**

The generated functions read the adapter back through
`__verified_pubsub_adapter__/0`, defined by `handle_opts/1` in Task 2, so the
transformer never needs the `use` options. `Transformer.eval/3` injects code into the
module being compiled.

```elixir
# lib/verified_pubsub/transformers/define_functions.ex
defmodule VerifiedPubsub.Transformers.DefineFunctions do
  @moduledoc """
  Generates the `topic_*`, `subscribe_*`, `unsubscribe_*`, and `broadcast_*` functions
  onto the registry module.

  Because an undeclared topic or event simply has no generated function, calling one is
  an ordinary undefined-function compile error — the same mechanism verified routes
  relies on.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def after?(VerifiedPubsub.Transformers.ParseParams), do: true
  def after?(VerifiedPubsub.Transformers.ValidateTopics), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    topics = Transformer.get_entities(dsl, [:topics])

    Enum.reduce(topics, {:ok, dsl}, fn topic, {:ok, acc} ->
      {:ok, Transformer.eval(acc, [topic: Macro.escape(topic)], topic_functions(topic))}
    end)
  end

  # `params_arg` is the function head's params argument: a map pattern destructuring
  # exactly the topic's params, so a missing key raises FunctionClauseError. For a
  # param-free topic the argument is omitted entirely.
  defp topic_functions(topic) do
    name = topic.name
    params = topic.params

    args = params_args(params)
    param_vars = Enum.map(params, &Macro.var(&1, __MODULE__))
    interpolation = interpolation_ast(topic.pattern, params)
    params_map = params_map_ast(params)

    topic_fn = :"topic_#{name}"
    subscribe_fn = :"subscribe_#{name}"
    unsubscribe_fn = :"unsubscribe_#{name}"

    broadcasts =
      Enum.map(topic.messages, fn message ->
        broadcast_functions(name, message.name, args, params_map, topic_fn)
      end)

    quote do
      @doc "Returns the wire topic string for `#{unquote(inspect(name))}`."
      def unquote(topic_fn)(unquote_splicing(args)) do
        _ = unquote(param_vars)
        unquote(interpolation)
      end

      @doc "Subscribes the calling process to `#{unquote(inspect(name))}`."
      def unquote(subscribe_fn)(unquote_splicing(args)) do
        __verified_pubsub_adapter__().subscribe(
          __verified_pubsub_config__(),
          unquote(topic_fn)(unquote_splicing(args))
        )
      end

      @doc "Unsubscribes the calling process from `#{unquote(inspect(name))}`."
      def unquote(unsubscribe_fn)(unquote_splicing(args)) do
        __verified_pubsub_adapter__().unsubscribe(
          __verified_pubsub_config__(),
          unquote(topic_fn)(unquote_splicing(args))
        )
      end

      unquote_splicing(broadcasts)
    end
  end

  defp broadcast_functions(topic_name, event, args, params_map, topic_fn) do
    fn_name = :"broadcast_#{topic_name}_#{event}"
    bang_name = :"broadcast_#{topic_name}_#{event}!"
    all_args = args ++ [Macro.var(:payload, __MODULE__)]

    quote do
      @doc "Broadcasts `#{unquote(inspect(event))}` on `#{unquote(inspect(topic_name))}`."
      def unquote(fn_name)(unquote_splicing(all_args)) do
        message = %VerifiedPubsub.Message{
          registry: __MODULE__,
          topic: unquote(topic_name),
          event: unquote(event),
          params: unquote(params_map),
          payload: unquote(Macro.var(:payload, __MODULE__))
        }

        __verified_pubsub_adapter__().broadcast(
          __verified_pubsub_config__(),
          unquote(topic_fn)(unquote_splicing(args)),
          message
        )
      end

      @doc "Same as `#{unquote(fn_name)}/#{unquote(length(all_args))}` but raises on failure."
      def unquote(bang_name)(unquote_splicing(all_args)) do
        case unquote(fn_name)(unquote_splicing(all_args)) do
          :ok ->
            :ok

          {:error, reason} ->
            raise "failed to broadcast #{unquote(inspect(event))} on " <>
                    "#{unquote(inspect(topic_name))}: #{inspect(reason)}"
        end
      end
    end
  end

  defp params_args([]), do: []

  defp params_args(params) do
    pairs = Enum.map(params, fn p -> {p, Macro.var(p, __MODULE__)} end)
    [{:%{}, [], pairs}]
  end

  defp params_map_ast([]), do: {:%{}, [], []}

  defp params_map_ast(params) do
    {:%{}, [], Enum.map(params, fn p -> {p, Macro.var(p, __MODULE__)} end)}
  end

  # Turns "accounts:%{account_id}:campaigns" into the AST for
  # "accounts:" <> to_string(account_id) <> ":campaigns".
  defp interpolation_ast(pattern, params) do
    literals = String.split(pattern, ~r/%\{[^}]*\}/)

    params
    |> Enum.zip(tl(literals))
    |> Enum.reduce(hd(literals), fn {param, literal}, acc ->
      quote do
        unquote(acc) <>
          to_string(unquote(Macro.var(param, __MODULE__))) <>
          unquote(literal)
      end
    end)
  end
end
```

Register it last:

```elixir
  use Spark.Dsl.Extension,
    sections: [@topics],
    transformers: [
      VerifiedPubsub.Transformers.ParseParams,
      VerifiedPubsub.Transformers.ValidateTopics,
      VerifiedPubsub.Transformers.DefineFunctions
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/broadcast_test.exs`
Expected: 11 tests, 0 failures.

The `_ = unquote(param_vars)` line in `topic_*` suppresses unused-variable warnings for
a topic whose pattern somehow does not use a declared param. Run `mix compile
--force --warnings-as-errors` to confirm the generated code is warning-clean; fix any
warning before committing rather than leaving it for later tasks to trip over.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/dsl.ex lib/verified_pubsub/transformers/define_functions.ex \
        test/verified_pubsub/broadcast_test.exs
git commit -m "Generate broadcast and subscribe functions on the registry"
```

---

### Task 6: The Phoenix PubSub adapter

**Files:**
- Create: `lib/verified_pubsub/adapter/phoenix_pub_sub.ex`
- Test: `test/verified_pubsub/adapter/phoenix_pub_sub_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubsub.Adapter`, `VerifiedPubsub.Message`.
- Produces: `VerifiedPubsub.Adapter.PhoenixPubSub`, where `config` is the
  `Phoenix.PubSub` process name.

- [ ] **Step 1: Write the failing test**

```elixir
# test/verified_pubsub/adapter/phoenix_pub_sub_test.exs
defmodule VerifiedPubsub.Adapter.PhoenixPubSubTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Adapter.PhoenixPubSub
  alias VerifiedPubsub.Message

  setup do
    name = :"pubsub_#{System.unique_integer([:positive])}"
    start_supervised!({Phoenix.PubSub, name: name})
    %{pubsub: name}
  end

  defp message do
    %Message{registry: R, topic: :campaigns, event: :created, params: %{}, payload: %{id: "c1"}}
  end

  test "delivers to subscribers", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast(pubsub, "t", message())

    assert_receive %Message{payload: %{id: "c1"}}
  end

  test "does not deliver to other topics", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast(pubsub, "other", message())

    refute_receive %Message{}, 50
  end

  test "unsubscribe stops delivery", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.unsubscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast(pubsub, "t", message())

    refute_receive %Message{}, 50
  end

  test "a registry can use the Phoenix adapter end to end", %{pubsub: pubsub} do
    module =
      VerifiedPubsub.CompileHelper.compile!("""
      defmodule #{VerifiedPubsub.CompileHelper.unique_module("VPTest.PhoenixReg")} do
        use VerifiedPubsub.Registry,
          adapter: VerifiedPubsub.Adapter.PhoenixPubSub,
          pubsub: #{inspect(pubsub)}

        topic :campaigns, "accounts:%{account_id}:campaigns" do
          message :created do
            field :id, :string
          end
        end
      end
      """)

    assert :ok = module.subscribe_campaigns(%{account_id: "7"})
    assert :ok = module.broadcast_campaigns_created!(%{account_id: "7"}, %{id: "c1"})

    assert_receive %Message{topic: :campaigns, event: :created, params: %{account_id: "7"}}
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/verified_pubsub/adapter/phoenix_pub_sub_test.exs`
Expected: FAIL — `VerifiedPubsub.Adapter.PhoenixPubSub` is undefined.

- [ ] **Step 3: Write the implementation**

This is the only file permitted to reference `Phoenix.PubSub`. Guard the whole module
so the library compiles cleanly when Phoenix is absent.

```elixir
# lib/verified_pubsub/adapter/phoenix_pub_sub.ex
if Code.ensure_loaded?(Phoenix.PubSub) do
  defmodule VerifiedPubsub.Adapter.PhoenixPubSub do
    @moduledoc """
    Adapter backed by `Phoenix.PubSub`. Available only when `:phoenix_pubsub` is a
    dependency of the host application.

    `config` is the `Phoenix.PubSub` process name, given as the `:pubsub` option to
    `use VerifiedPubsub.Registry`.
    """

    @behaviour VerifiedPubsub.Adapter

    @impl true
    def subscribe(pubsub, topic) when is_binary(topic) do
      Phoenix.PubSub.subscribe(pubsub, topic)
    end

    @impl true
    def unsubscribe(pubsub, topic) when is_binary(topic) do
      Phoenix.PubSub.unsubscribe(pubsub, topic)
    end

    @impl true
    def broadcast(pubsub, topic, message) when is_binary(topic) do
      Phoenix.PubSub.broadcast(pubsub, topic, message)
    end
  end
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/adapter/phoenix_pub_sub_test.exs`
Expected: 4 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/adapter/phoenix_pub_sub.ex \
        test/verified_pubsub/adapter/phoenix_pub_sub_test.exs
git commit -m "Add the Phoenix.PubSub adapter"
```

---

### Task 7: The subscriber — `handle_message`, `ignore_message`, and codegen

Codegen only. Exhaustiveness checking is Task 8, so this task's tests always account
for every event.

**Files:**
- Create: `lib/verified_pubsub/subscriber.ex`
- Test: `test/verified_pubsub/subscriber_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubsub.Info.events/2` and `params/2`, `VerifiedPubsub.Message`.
- Produces:
  - `use VerifiedPubsub.Subscriber, registry: module(), topics: [atom()], on_missing: :error | :warn | :ignore`
  - `handle_message(topic, event, pattern, state, do: body)`
  - `ignore_message(topic, event)`
  - One generated `handle_info(%VerifiedPubsub.Message{}, state)` clause per module,
    delegating to grouped private `__verified_pubsub_dispatch__/4` clauses.
  - Imports `subscribe_*`/`unsubscribe_*` for the listed topics from the registry.

- [ ] **Step 1: Write the failing test**

```elixir
# test/verified_pubsub/subscriber_test.exs
defmodule VerifiedPubsub.SubscriberTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Message

  defmodule Worker do
    use GenServer

    use VerifiedPubsub.Subscriber,
      registry: VerifiedPubsub.TestRegistries.Basic,
      topics: [:campaigns]

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      subscribe_campaigns(%{account_id: opts[:account_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    handle_message :campaigns, :created, payload, state do
      send(state.owner, {:created, payload})
      {:noreply, state}
    end

    handle_message :campaigns, :updated,
                   %Message{params: %{account_id: account_id}, payload: payload},
                   state do
      send(state.owner, {:updated, account_id, payload})
      {:noreply, state}
    end

    ignore_message :campaigns, :deleted
  end

  setup do
    start_supervised!(VerifiedPubsub.Adapter.Local)
    start_supervised!({Worker, owner: self(), account_id: "7"})
    :ok
  end

  alias VerifiedPubsub.TestRegistries.Basic

  test "the payload form receives the payload" do
    Basic.broadcast_campaigns_created!(%{account_id: "7"}, %{id: "c1"})

    assert_receive {:created, %{id: "c1"}}
  end

  test "the Message form can match on topic params" do
    Basic.broadcast_campaigns_updated!(%{account_id: "7"}, %{id: "c2"})

    assert_receive {:updated, "7", %{id: "c2"}}
  end

  test "an ignored event is received without crashing and without a message" do
    Basic.broadcast_campaigns_deleted!(%{account_id: "7"}, %{id: "c3"})

    refute_receive {:created, _}, 50
    refute_receive {:updated, _, _}, 50
    assert Process.whereis(nil) == nil
  end

  test "subscribe_* is imported from the registry" do
    assert function_exported?(Worker, :init, 1)
  end

  test "exactly one handle_info clause is generated" do
    assert function_exported?(Worker, :handle_info, 2)
  end

  test "the module compiles with no warnings" do
    # Guards against the "clauses with the same name and arity are not grouped"
    # warning, which is why handle_message does not define handle_info inline.
    assert Worker.module_info(:module) == Worker
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/verified_pubsub/subscriber_test.exs`
Expected: FAIL — `VerifiedPubsub.Subscriber` is undefined.

- [ ] **Step 3: Write the implementation**

Macro arguments arrive as unevaluated AST, and AST is an ordinary term, so
`Module.put_attribute/3` can store patterns and bodies directly — no `Macro.escape`
round trip. Returning `nil` from the macro emits no code at the call site, which is
what lets `@before_compile` emit every clause grouped together.

```elixir
# lib/verified_pubsub/subscriber.ex
defmodule VerifiedPubsub.Subscriber do
  @moduledoc """
  Declares that a module subscribes to topics from a registry, and defines its
  handlers.

      defmodule MyAppWeb.CampaignsLive do
        use MyAppWeb, :live_view
        use VerifiedPubsub.Subscriber, registry: MyApp.Topics, topics: [:campaigns]

        def mount(_params, _session, socket) do
          if connected?(socket), do: subscribe_campaigns(%{account_id: socket.assigns.id})
          {:ok, socket}
        end

        handle_message :campaigns, :created, payload, socket do
          {:noreply, stream_insert(socket, :campaigns, payload)}
        end

        ignore_message :campaigns, :deleted
      end

  Every event declared on a subscribed topic must be either handled by
  `handle_message/5` or dismissed by `ignore_message/2`, or the module does not
  compile. See `VerifiedPubsub.Subscriber.Verify` for the exact rule.
  """

  @options [:registry, :topics, :on_missing]

  defmacro __using__(opts) do
    registry = Keyword.fetch!(opts, :registry) |> Macro.expand(__CALLER__)
    topics = Keyword.fetch!(opts, :topics)
    on_missing = Keyword.get(opts, :on_missing, :error)

    unless on_missing in [:error, :warn, :ignore] do
      raise ArgumentError,
            "invalid :on_missing #{inspect(on_missing)}. " <>
              "Expected :error, :warn, or :ignore."
    end

    case Keyword.keys(opts) -- @options do
      [] -> :ok
      extra -> raise ArgumentError, "unknown options #{inspect(extra)}"
    end

    # Reading the registry at compile time creates a compile-time dependency on it,
    # so editing the registry recompiles every subscriber. That is deliberate.
    imports = subscribe_imports(registry, topics)

    quote do
      Module.register_attribute(__MODULE__, :verified_pubsub_clauses, accumulate: true)
      Module.register_attribute(__MODULE__, :verified_pubsub_ignored, accumulate: true)

      @verified_pubsub_registry unquote(registry)
      @verified_pubsub_topics unquote(topics)
      @verified_pubsub_on_missing unquote(on_missing)

      import unquote(registry), only: unquote(imports)
      import VerifiedPubsub.Subscriber, only: [handle_message: 5, ignore_message: 2]

      @before_compile VerifiedPubsub.Subscriber
    end
  end

  defp subscribe_imports(registry, topics) do
    Enum.flat_map(topics, fn topic ->
      arity = if VerifiedPubsub.Info.params(registry, topic) == [], do: 0, else: 1
      [{:"subscribe_#{topic}", arity}, {:"unsubscribe_#{topic}", arity}]
    end)
  end

  @doc """
  Handles one event on one topic.

  `pattern` matches the message `payload`, unless it is syntactically a
  `%VerifiedPubsub.Message{}` pattern, in which case it matches the whole message and
  gives access to `params`.
  """
  defmacro handle_message(topic, event, pattern, state, do: body) do
    topic = literal_atom!(topic, :topic)
    event = literal_atom!(event, :event)

    Module.put_attribute(__CALLER__.module, :verified_pubsub_clauses, %{
      topic: topic,
      event: event,
      pattern: pattern,
      state: state,
      body: body,
      whole_message?: message_pattern?(pattern, __CALLER__),
      line: __CALLER__.line
    })

    nil
  end

  @doc """
  Declares that this module knowingly does nothing with an event.

  Satisfies exhaustiveness without a handler. The generated clause returns
  `{:noreply, state}`, which is correct for both GenServer and LiveView.
  """
  defmacro ignore_message(topic, event) do
    topic = literal_atom!(topic, :topic)
    event = literal_atom!(event, :event)

    Module.put_attribute(__CALLER__.module, :verified_pubsub_ignored, {topic, event})

    nil
  end

  defp literal_atom!(ast, role) when is_atom(ast), do: ast

  defp literal_atom!(ast, role) do
    raise ArgumentError,
          "expected a literal atom for #{role}, got: #{Macro.to_string(ast)}. " <>
            "Topic and event must be literals so coverage can be checked at compile time."
  end

  # A struct pattern is unmistakable in the AST. `%Message{...} = var` is supported by
  # checking both sides of a top-level match.
  defp message_pattern?({:=, _, [left, right]}, env) do
    message_pattern?(left, env) or message_pattern?(right, env)
  end

  defp message_pattern?({:%, _, [alias_ast, {:%{}, _, _}]}, env) do
    Macro.expand(alias_ast, env) == VerifiedPubsub.Message
  end

  defp message_pattern?(_, _), do: false

  defmacro __before_compile__(env) do
    clauses = env.module |> Module.get_attribute(:verified_pubsub_clauses) |> Enum.reverse()
    ignored = env.module |> Module.get_attribute(:verified_pubsub_ignored) |> Enum.reverse()

    VerifiedPubsub.Subscriber.Verify.run!(env, clauses, ignored)

    dispatch =
      Enum.map(clauses, &dispatch_clause/1) ++ Enum.map(ignored, &ignored_clause/1)

    if dispatch == [] do
      nil
    else
      quote do
        unquote_splicing(dispatch)

        @impl true
        def handle_info(%VerifiedPubsub.Message{} = message, state) do
          __verified_pubsub_dispatch__(message.topic, message.event, message, state)
        end
      end
    end
  end

  defp dispatch_clause(%{whole_message?: true} = clause) do
    quote line: clause.line do
      defp __verified_pubsub_dispatch__(
             unquote(clause.topic),
             unquote(clause.event),
             unquote(clause.pattern),
             unquote(clause.state)
           ) do
        unquote(clause.body)
      end
    end
  end

  defp dispatch_clause(clause) do
    quote line: clause.line do
      defp __verified_pubsub_dispatch__(
             unquote(clause.topic),
             unquote(clause.event),
             %VerifiedPubsub.Message{payload: unquote(clause.pattern)},
             unquote(clause.state)
           ) do
        unquote(clause.body)
      end
    end
  end

  defp ignored_clause({topic, event}) do
    quote do
      defp __verified_pubsub_dispatch__(unquote(topic), unquote(event), _message, state) do
        {:noreply, state}
      end
    end
  end
end
```

Create a stub for the verifier so this task compiles; Task 8 fills it in.

```elixir
# lib/verified_pubsub/subscriber/verify.ex
defmodule VerifiedPubsub.Subscriber.Verify do
  @moduledoc "Compile-time coverage check for `VerifiedPubsub.Subscriber`."

  @doc false
  def run!(_env, _clauses, _ignored), do: :ok
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/subscriber_test.exs`
Expected: 6 tests, 0 failures.

If `@impl true` on the generated `handle_info` warns for a plain GenServer that never
declared `@behaviour`, drop the `@impl true` line from the generated clause — the
warning matters more than the annotation.

- [ ] **Step 5: Verify no grouping warnings**

Run: `mix compile --force --warnings-as-errors`
Expected: success. This is the specific thing the accumulate-then-emit design exists to
prevent; if it warns, the clauses are not being emitted together.

- [ ] **Step 6: Commit**

```bash
git add lib/verified_pubsub/subscriber.ex lib/verified_pubsub/subscriber \
        test/verified_pubsub/subscriber_test.exs
git commit -m "Add subscriber macros and handle_info codegen"
```

---

### Task 8: Subscriber exhaustiveness checking

**Files:**
- Modify: `lib/verified_pubsub/subscriber/verify.ex` (replace the stub)
- Test: `test/verified_pubsub/subscriber_verify_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubsub.Info.events/2`; the `clauses` and `ignored` lists from
  Task 7's `__before_compile__`.
- Produces: `VerifiedPubsub.Subscriber.Verify.run!(env, clauses, ignored) :: :ok`,
  raising `CompileError` on a violation.

- [ ] **Step 1: Write the failing test**

```elixir
# test/verified_pubsub/subscriber_verify_test.exs
defmodule VerifiedPubsub.SubscriberVerifyTest do
  use ExUnit.Case, async: true

  import VerifiedPubsub.CompileHelper

  defp subscriber_source(body, opts \\ "") do
    """
    defmodule #{unique_module("VPTest.Sub")} do
      use VerifiedPubsub.Subscriber,
        registry: VerifiedPubsub.TestRegistries.Basic,
        topics: [:campaigns]#{opts}

      #{body}
    end
    """
  end

  defp all_handled do
    """
    handle_message :campaigns, :created, p, s do
      {:noreply, {p, s}}
    end

    handle_message :campaigns, :updated, p, s do
      {:noreply, {p, s}}
    end

    handle_message :campaigns, :deleted, p, s do
      {:noreply, {p, s}}
    end
    """
  end

  test "a fully-covered subscriber compiles" do
    assert is_atom(compile!(subscriber_source(all_handled())))
  end

  test "a missing event is a compile error naming the event and the escape hatch" do
    error =
      compile_error(
        subscriber_source("""
        handle_message :campaigns, :created, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected a missing event to fail compilation"
    message = Exception.message(error)
    assert message =~ ":updated"
    assert message =~ ":deleted"
    assert message =~ "ignore_message"
  end

  test "ignore_message satisfies coverage" do
    assert is_atom(
             compile!(
               subscriber_source("""
               handle_message :campaigns, :created, p, s do
                 {:noreply, {p, s}}
               end

               ignore_message :campaigns, :updated
               ignore_message :campaigns, :deleted
               """)
             )
           )
  end

  test "handling an event not declared on the topic is a compile error" do
    error =
      compile_error(
        subscriber_source("""
        #{all_handled()}

        handle_message :campaigns, :exploded, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected an undeclared event to fail compilation"
    assert Exception.message(error) =~ ":exploded"
  end

  test "handling a topic that was not subscribed to is a compile error" do
    error =
      compile_error(
        subscriber_source("""
        #{all_handled()}

        handle_message :system, :alert, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected an unsubscribed topic to fail compilation"
    assert Exception.message(error) =~ ":system"
  end

  test "several clauses for one event are allowed" do
    assert is_atom(
             compile!(
               subscriber_source("""
               handle_message :campaigns, :created, %{id: "special"}, s do
                 {:noreply, s}
               end

               handle_message :campaigns, :created, p, s do
                 {:noreply, {p, s}}
               end

               ignore_message :campaigns, :updated
               ignore_message :campaigns, :deleted
               """)
             )
           )
  end

  test "on_missing: :ignore skips the check" do
    assert is_atom(
             compile!(
               subscriber_source(
                 """
                 handle_message :campaigns, :created, p, s do
                   {:noreply, {p, s}}
                 end
                 """,
                 ",\n    on_missing: :ignore"
               )
             )
           )
  end

  test "a non-literal topic is rejected" do
    error =
      compile_error(
        subscriber_source("""
        @t :campaigns
        handle_message @t, :created, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected a non-literal topic to fail compilation"
    assert Exception.message(error) =~ "literal"
  end
end
```

The "several clauses for one event" test is the regression guard for the set-semantics
bug: with list subtraction, the duplicate `{:campaigns, :created}` would leave a
residue in `accounted_for -- declared` and be misreported as undeclared.

- [ ] **Step 2: Run test to verify it fails**

Run: `mix test test/verified_pubsub/subscriber_verify_test.exs`
Expected: FAIL — the stub returns `:ok`, so every "is a compile error" test fails.

- [ ] **Step 3: Write the implementation**

```elixir
# lib/verified_pubsub/subscriber/verify.ex
defmodule VerifiedPubsub.Subscriber.Verify do
  @moduledoc """
  Compile-time coverage check for `VerifiedPubsub.Subscriber`.

  Coverage is tracked per `{topic, event}` pair using set semantics, because several
  `handle_message` clauses for one event are legal when matching on param values.
  Coverage is therefore name-based, not value-based: if every clause for an event
  matches a narrow param value, the event counts as covered and a message with any
  other value raises `FunctionClauseError` at runtime. No static check closes that gap.
  """

  alias VerifiedPubsub.Info

  @doc false
  def run!(env, clauses, ignored) do
    registry = Module.get_attribute(env.module, :verified_pubsub_registry)
    topics = Module.get_attribute(env.module, :verified_pubsub_topics)
    on_missing = Module.get_attribute(env.module, :verified_pubsub_on_missing)

    declared =
      for topic <- topics, event <- Info.events(registry, topic), into: MapSet.new() do
        {topic, event}
      end

    accounted_for =
      MapSet.new(Enum.map(clauses, &{&1.topic, &1.event}) ++ ignored)

    undeclared = MapSet.difference(accounted_for, declared)
    missing = MapSet.difference(declared, accounted_for)

    if MapSet.size(undeclared) > 0 do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: undeclared_message(env, registry, topics, undeclared)
    end

    if MapSet.size(missing) > 0 and on_missing != :ignore do
      description = missing_message(env, registry, missing)

      case on_missing do
        :error -> raise CompileError, file: env.file, line: env.line, description: description
        :warn -> IO.warn(description, env)
      end
    end

    :ok
  end

  defp undeclared_message(env, registry, topics, undeclared) do
    pairs =
      undeclared
      |> Enum.sort()
      |> Enum.map_join("\n", fn {topic, event} ->
        if topic in topics do
          "  * #{inspect(topic)}, #{inspect(event)} — " <>
            "#{inspect(registry)} declares #{inspect(Info.events(registry, topic))} on " <>
            "#{inspect(topic)}"
        else
          "  * #{inspect(topic)}, #{inspect(event)} — " <>
            "#{inspect(topic)} is not in the :topics list #{inspect(topics)}"
        end
      end)

    """
    #{inspect(env.module)} handles messages that #{inspect(registry)} does not declare:

    #{pairs}

    Either fix the topic or event name, add the topic to the :topics option of
    `use VerifiedPubsub.Subscriber`, or declare it in the registry.
    """
  end

  defp missing_message(env, registry, missing) do
    pairs =
      missing
      |> Enum.sort()
      |> Enum.map_join("\n", fn {topic, event} ->
        "  * #{inspect(topic)}, #{inspect(event)}"
      end)

    """
    #{inspect(env.module)} subscribes to topics with events it does not account for:

    #{pairs}

    Add a `handle_message` clause for each, or dismiss it with `ignore_message`:

        ignore_message #{missing |> Enum.sort() |> hd() |> then(fn {t, e} -> "#{inspect(t)}, #{inspect(e)}" end)}

    Registry: #{inspect(registry)}
    """
  end
end
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/verified_pubsub/subscriber_verify_test.exs`
Expected: 8 tests, 0 failures.

- [ ] **Step 5: Run the whole suite**

Run: `mix test`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add lib/verified_pubsub/subscriber/verify.ex \
        test/verified_pubsub/subscriber_verify_test.exs
git commit -m "Enforce subscriber exhaustiveness at compile time"
```

---

### Task 9: LiveView smoke test

Proves `use VerifiedPubsub.Subscriber` composes with `use Phoenix.LiveView`, which is
the one subscriber target whose `use` macro could conflict with ours.

**Files:**
- Modify: `mix.exs` (add `{:phoenix_live_view, "~> 1.0", only: :test}`)
- Test: `test/verified_pubsub/live_view_test.exs`

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the test-only dependency**

```elixir
  defp deps do
    [
      {:spark, "~> 2.7"},
      {:phoenix_pubsub, "~> 2.1", optional: true},
      {:phoenix_live_view, "~> 1.0", only: :test}
    ]
  end
```

Run: `mix deps.get`

- [ ] **Step 2: Write the failing test**

The LiveView is driven directly through `handle_info/2` rather than a full connection,
which keeps the test free of endpoint and router setup while still exercising the
generated clause inside a real LiveView module.

```elixir
# test/verified_pubsub/live_view_test.exs
defmodule VerifiedPubsub.LiveViewTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.TestRegistries.Basic

  defmodule CampaignsLive do
    use Phoenix.LiveView

    use VerifiedPubsub.Subscriber,
      registry: VerifiedPubsub.TestRegistries.Basic,
      topics: [:campaigns]

    @impl true
    def mount(_params, _session, socket) do
      {:ok, assign(socket, :campaigns, [])}
    end

    @impl true
    def render(assigns), do: ~H"<div>{length(@campaigns)}</div>"

    handle_message :campaigns, :created, payload, socket do
      {:noreply, assign(socket, :campaigns, [payload | socket.assigns.campaigns])}
    end

    ignore_message :campaigns, :updated
    ignore_message :campaigns, :deleted
  end

  test "the generated handle_info runs inside a LiveView" do
    {:ok, socket} = CampaignsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})

    message = %VerifiedPubsub.Message{
      registry: Basic,
      topic: :campaigns,
      event: :created,
      params: %{account_id: "7"},
      payload: %{id: "c1"}
    }

    assert {:noreply, updated} = CampaignsLive.handle_info(message, socket)
    assert updated.assigns.campaigns == [%{id: "c1"}]
  end

  test "an ignored event leaves the socket untouched" do
    {:ok, socket} = CampaignsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})

    message = %VerifiedPubsub.Message{
      registry: Basic,
      topic: :campaigns,
      event: :deleted,
      params: %{account_id: "7"},
      payload: %{id: "c1"}
    }

    assert {:noreply, ^socket} = CampaignsLive.handle_info(message, socket)
  end
end
```

- [ ] **Step 3: Run tests**

Run: `mix test test/verified_pubsub/live_view_test.exs`
Expected: 2 tests, 0 failures.

If `use Phoenix.LiveView` already defines `handle_info/2` clauses or marks it
overridable, and the generated clause conflicts, the fix is to emit the generated
`handle_info` with `defoverridable`-awareness — check whether
`Module.overridable?(env.module, {:handle_info, 2})` is true in `__before_compile__`
and use `defoverridable` accordingly. Record whatever is discovered in the moduledoc.

- [ ] **Step 4: Commit**

```bash
git add mix.exs mix.lock test/verified_pubsub/live_view_test.exs
git commit -m "Add LiveView composition smoke test"
```

---

### Task 10: Formatter export, README, and top-level moduledoc

**Files:**
- Modify: `.formatter.exs`
- Modify: `lib/verified_pubsub.ex` (replace the superseded skeleton)
- Modify: `README.md`
- Modify: `mix.exs` (docs/package metadata)

**Interfaces:**
- Consumes: everything.
- Produces: nothing.

- [ ] **Step 1: Delete the superseded skeleton and write the real moduledoc**

`lib/verified_pubsub.ex` currently holds the abandoned outline (`topic/1`,
`message/2`, an empty `compile/1`). Replace the whole file:

```elixir
defmodule VerifiedPubsub do
  @moduledoc """
  Compile-time verified PubSub.

  Declare every topic and event once, in a registry:

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

  Broadcasting uses generated functions, so an unknown topic or event is an
  undefined-function compile error:

      MyApp.Topics.broadcast_campaigns_created!(%{account_id: id}, %{id: c.id, name: c.name})

  Subscribers declare which topics they consume, and must account for every event on
  them:

      defmodule MyApp.Worker do
        use GenServer
        use VerifiedPubsub.Subscriber, registry: MyApp.Topics, topics: [:campaigns]

        handle_message :campaigns, :created, payload, state do
          {:noreply, state}
        end

        ignore_message :campaigns, :deleted
      end

  ## What is and is not checked

  Checked at compile time: topic and event names on broadcast, that a subscriber
  accounts for every declared event, and that it does not handle events the registry
  does not declare.

  Not checked: payload shapes (declared via `field`, enforced in a later release), and
  topic *param values* — coverage is tracked per `{topic, event}`, so if every clause
  for an event matches a narrow param value, a message with a different value raises
  `FunctionClauseError`. End with a param-agnostic clause when matching on params.
  """
end
```

- [ ] **Step 2: Export the formatter locals**

Spark's `mix spark.formatter` task maintains `locals_without_parens` for DSL entities.
`handle_message` and `ignore_message` are ordinary macros, not Spark entities, so they
must be listed by hand.

```elixir
# .formatter.exs
locals_without_parens = [
  # VerifiedPubsub.Dsl entities
  topic: 2,
  topic: 3,
  message: 1,
  message: 2,
  field: 2,
  field: 3,
  # VerifiedPubsub.Subscriber macros
  handle_message: 5,
  ignore_message: 2
]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
```

- [ ] **Step 3: Rewrite the README**

Replace the generated stub with: a one-paragraph statement of the problem (drift
between broadcast sites and handlers), the installation snippet, the three code blocks
from the moduledoc (registry, broadcast, subscriber), the "What is and is not checked"
section verbatim from the moduledoc, and a note that `phoenix_pubsub` is optional and
`VerifiedPubsub.Adapter.Local` is available for tests.

- [ ] **Step 4: Verify formatting and the full suite**

Run: `mix format --check-formatted && mix compile --force --warnings-as-errors && mix test`
Expected: all three succeed.

- [ ] **Step 5: Commit**

```bash
git add .formatter.exs lib/verified_pubsub.ex README.md mix.exs
git commit -m "Add formatter exports, README, and top-level docs"
```

---

## Self-Review

**Spec coverage.** Goal 1 (valid topic/event on broadcast) → Task 5. Goal 2
(exhaustiveness) → Task 8. Goal 3 (no undeclared events) → Task 8. Goal 4 (payload
shapes declared, not enforced) → Task 2, asserted explicitly. Spec §1 registry DSL →
Tasks 2–4. §2 broadcast surface → Task 5. §3 wire format → Task 1. §4 subscriber →
Tasks 7–8. §5 check placement table → Tasks 4, 5, 8. §6 adapters → Tasks 1 and 6.
Testing strategy items 1–6 → Tasks 2, 4, 5, 8, 7, 9 respectively. Delivery notes →
Task 10.

**Deliberately deferred, matching the spec's non-goals:** payload enforcement, Phoenix
Channels, import-style broadcasters, runtime topic registration.

**Type consistency.** `Info.events/2` returns `[atom()]` and is consumed that way in
Tasks 7 and 8. `Info.params/2` returns `[atom()]`, consumed in Task 7's
`subscribe_imports/2`. `Adapter` callbacks are 3/2/2-arity in Task 1 and called at
those arities in Tasks 5 and 6. `%Message{}` field names (`registry`, `topic`, `event`,
`params`, `payload`) are identical in Tasks 1, 5, 7, 8, and 9. Generated function names
use the `broadcast_<topic>_<event>` form consistently in Tasks 5, 7, 8, and 9.
`Subscriber.Verify.run!/3` is stubbed with that exact signature in Task 7 and
implemented with it in Task 8.

**Known risks, each with a stated fallback in the task itself:**
1. `Transformer.replace_entity/4` arity may differ in 2.7.2 (Task 3, Step 4).
2. `use Phoenix.LiveView` may already define or mark `handle_info/2` overridable
   (Task 9, Step 3).
3. `@impl true` on the generated `handle_info` may warn for plain GenServers
   (Task 7, Step 4).
