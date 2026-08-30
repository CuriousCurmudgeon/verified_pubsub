defmodule VerifiedPubSub do
  @moduledoc """
  Compile-time verified PubSub.

  A broadcast and its handler are normally two string literals in two files with
  nothing tying them together, so renaming or deleting an event leaves dead handlers
  and unhandled messages behind, silently. `VerifiedPubSub` makes a registry the single
  source of truth and turns that drift into compile-time failures.

  ## The registry

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

  `:campaigns` is an alias used to build function names; the string is the wire topic.
  `%{account_id}` marks a parameter, and the parameter list is derived from the pattern
  rather than declared twice.

  ## Broadcasting

      defmodule MyApp.Campaigns do
        use VerifiedPubSub, registry: MyApp.Topics

        def create(attrs) do
          # ...
          broadcast!(:campaigns, :created, %{account_id: attrs.account_id}, payload)
        end
      end

  `use VerifiedPubSub, registry: ...` imports `VerifiedPubSub.Api`. Topic and event are
  ordinary arguments, but because these are macros they must be **literal atoms** — that
  is what makes a typo a compile error. Params may be built at runtime.

  A topic with no params takes an empty map: `broadcast!(:system, :alert, %{}, payload)`.

  To skip the sender, use `broadcast_from!/5`, which mirrors
  `Phoenix.PubSub.broadcast_from/4` (`from` leads, as it does there):

      broadcast_from!(self(), :campaigns, :created, %{account_id: id}, payload)

  ## Subscribing

      defmodule MyApp.Worker do
        use GenServer
        use VerifiedPubSub.Subscriber, registry: MyApp.Topics, topics: [:campaigns]

        def init(account_id) do
          :ok = subscribe(:campaigns, %{account_id: account_id})
          {:ok, account_id}
        end

        handle_message :campaigns, :created, payload, state do
          {:noreply, state}
        end

        ignore_message :campaigns, :deleted
      end

  Every event declared on a subscribed topic must be handled or explicitly dismissed.
  `ignore_message/2` exists because subscribers routinely care about a subset of a
  topic's events; without it, exhaustiveness would be unusable rather than merely
  strict.

  To match on topic params, pattern match the whole message instead of the payload:

      handle_message :campaigns, :created,
                     %VerifiedPubSub.Message{params: %{account_id: id}, payload: p},
                     state do
        {:noreply, state}
      end

  ## What is and is not checked

  Enforced as a hard compile error:

    * a subscriber that does not account for every event on a topic it subscribes to
    * a subscriber that handles an event the registry does not declare, or a topic not
      in its `:topics` list
    * duplicate topics, duplicate events on one topic, and malformed topic patterns

    * broadcasting an unknown topic, an unknown event, or an event that belongs to a
      different topic
    * a literal params map with missing or unexpected keys

  A params map built at runtime cannot be checked at compile time; `Map.fetch!/2` raises
  `KeyError` for a missing key instead.

  Not checked:

    * **Payload shapes.** `field` declarations are parsed and introspectable via
      `VerifiedPubSub.Info`, but nothing validates a payload against them yet.
    * **Topic param values.** Coverage is tracked per `{topic, event}` pair, so if
      every clause for an event matches a narrow param value, the event still counts as
      covered and a message with a different value raises `FunctionClauseError`. End
      with a param-agnostic clause when matching on param values.

  ## Transport

  Broadcasts and subscriptions go through `Phoenix.PubSub`, so `:pubsub` names a
  `Phoenix.PubSub` started in your supervision tree:

      children = [{Phoenix.PubSub, name: MyApp.PubSub}]

  There is deliberately no adapter layer here. `Phoenix.PubSub` already has its own
  adapter behaviour — that is where PG2, Redis, and anything else are configured — so
  wrapping it would duplicate an extension point one layer down, and leave you
  configuring transport in two places.

  ## Why macros

  The call-site API is macros rather than functions so that the topic and event can be
  checked while your code compiles. Plain functions taking atoms cannot be: Elixir's
  type inference does not narrow across clause heads on a remote call, so
  `broadcast(:campaigns, :creatd, ...)` would fail only at runtime.

  The cost is that every calling module needs `use VerifiedPubSub, registry: ...`, and
  macros cannot be piped into, captured with `&`, or called via `apply/3`. Modules that
  `use VerifiedPubSub.Subscriber` already have the import.
  """

  @doc """
  Imports the atom-first macros in `VerifiedPubSub.Api`, bound to `registry`.
  """
  defmacro __using__(opts) do
    registry = opts |> Keyword.fetch!(:registry) |> Macro.expand(__CALLER__)
    module = __CALLER__.module

    # Set during expansion, not from inside the quote: Elixir expands the macros in a
    # module body before the body's runtime calls execute, so an assignment in the quote
    # would not be visible to a `broadcast!` further down the same module.
    case Module.get_attribute(module, :verified_pubsub_registry) do
      nil ->
        Module.put_attribute(module, :verified_pubsub_registry, registry)

      ^registry ->
        :ok

      other ->
        raise ArgumentError,
              "#{inspect(module)} is already bound to registry #{inspect(other)}, " <>
                "cannot also bind #{inspect(registry)}"
    end

    quote do
      import VerifiedPubSub.Api
    end
  end
end
