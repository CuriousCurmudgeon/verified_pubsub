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

      MyApp.Topics.broadcast_campaigns_created!(%{account_id: id}, %{id: c.id, name: c.name})

  Params are passed as a map, which the generated function head destructures. A topic
  with no params takes only a payload: `MyApp.Topics.broadcast_system_alert!(payload)`.

  To skip the sender, use the `_from` variants, which mirror
  `Phoenix.PubSub.broadcast_from/4` (`from` leads, as it does there):

      MyApp.Topics.broadcast_campaigns_created_from!(self(), %{account_id: id}, payload)

  Each event generates `broadcast_*`, `broadcast_*!`, `broadcast_*_from`, and
  `broadcast_*_from!`.

  ## Subscribing

      defmodule MyApp.Worker do
        use GenServer
        use VerifiedPubSub.Subscriber, registry: MyApp.Topics, topics: [:campaigns]

        def init(account_id) do
          :ok = subscribe_campaigns(%{account_id: account_id})
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

  Enforced as a compile *warning*, promoted to an error by
  `mix compile --warnings-as-errors`:

    * broadcasting an unknown topic or event. This works by the generated function not
      existing, and Elixir reports an undefined remote function as a warning — one that
      helpfully lists the valid alternatives. Run `--warnings-as-errors` in CI to make
      it binding.
    * a params map with the wrong keys, when the map is a literal. Elixir's type
      inference catches it against the destructured function head. A dynamically-built
      map raises `FunctionClauseError` at runtime instead.

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
  """
end
