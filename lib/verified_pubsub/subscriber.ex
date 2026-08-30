defmodule VerifiedPubSub.Subscriber do
  @moduledoc """
  Declares that a module subscribes to topics from a registry, and defines its
  handlers.

      defmodule MyAppWeb.CampaignsLive do
        use MyAppWeb, :live_view
        use VerifiedPubSub.Subscriber, registry: MyApp.Topics

        def mount(_params, _session, socket) do
          if connected?(socket), do: subscribe(:campaigns, %{account_id: socket.assigns.id})
          {:ok, socket}
        end

        handle_message :campaigns, :created, payload, socket do
          {:noreply, stream_insert(socket, :campaigns, payload)}
        end

        ignore_message :campaigns, :deleted
      end

  The topics this module subscribes to are **inferred** from its `handle_message/5` and
  `ignore_message/2` calls — there is no list to keep in sync. Every event declared on
  each of those topics must then be either handled or dismissed, or the module does not
  compile.

  `use` also imports `VerifiedPubSub.Api`, so `subscribe/2`, `broadcast!/4` and the rest
  are available without a separate `use VerifiedPubSub, registry: ...`.

  ## Generated code

  A single `handle_info/2` clause matching `VerifiedPubSub.Message` is generated at the
  `use` site. It delegates to private `__verified_pubsub_dispatch__/4` clauses, which
  are emitted together at `@before_compile` — never inline at each `handle_message`, so
  they cannot trip Elixir's "clauses with the same name and arity should be grouped
  together" warning.

  Emitting at the `use` site rather than at `@before_compile` is what makes your own
  `handle_info/2` clauses work: they are matched *after* the generated one, so a
  catch-all of yours receives every non-verified message without shadowing verified
  ones.

  ## Messages this module does not expect

  Defining any `handle_info/2` clause discards the default that `use GenServer` and
  `use Phoenix.LiveView` install, so an unexpected message raises `FunctionClauseError`
  instead of being logged. That is normal for any GenServer with a custom
  `handle_info/2`, and it cannot be avoided here: Elixir 1.20 made `super/2` for
  GenServer callbacks a hard error, so the default body is no longer reachable.

  If your process receives messages other than verified ones, add your own catch-all:

      handle_message :campaigns, :created, payload, state do
        {:noreply, state}
      end

      # Matched after the generated clause, so verified messages still reach it.
      def handle_info(_other, state), do: {:noreply, state}
  """

  @options [:registry, :on_missing]

  defmacro __using__(opts) do
    registry = opts |> Keyword.fetch!(:registry) |> Macro.expand(__CALLER__)
    on_missing = Keyword.get(opts, :on_missing, :error)

    if Keyword.has_key?(opts, :topics) do
      raise ArgumentError,
            "the :topics option was removed. The topics a module subscribes to are now " <>
              "inferred from its handle_message/5 and ignore_message/2 calls."
    end

    case Keyword.keys(opts) -- @options do
      [] ->
        :ok

      extra ->
        raise ArgumentError,
              "unknown options #{inspect(extra)} given to use VerifiedPubSub.Subscriber. " <>
                "Expected only #{inspect(@options)}."
    end

    unless on_missing in [:error, :warn, :ignore] do
      raise ArgumentError,
            "invalid :on_missing #{inspect(on_missing)}. Expected :error, :warn, or :ignore."
    end

    module = __CALLER__.module

    # These MUST be set during expansion rather than from inside the quote below.
    # Elixir expands the macros in a module body before the body's runtime calls
    # execute, so a `Module.register_attribute` sitting in the quote would not be in
    # effect when `handle_message` expands: the puts would overwrite each other and the
    # later registration would then reset the attribute to []. Registering here means
    # accumulation is live before any `handle_message` in the body is expanded.
    Module.register_attribute(module, :verified_pubsub_clauses, accumulate: true)
    Module.register_attribute(module, :verified_pubsub_ignored, accumulate: true)
    Module.put_attribute(module, :verified_pubsub_registry, registry)
    Module.put_attribute(module, :verified_pubsub_on_missing, on_missing)

    quote do
      import VerifiedPubSub.Api
      import VerifiedPubSub.Subscriber, only: [handle_message: 5, ignore_message: 2]

      @before_compile VerifiedPubSub.Subscriber

      # Defined here, not at @before_compile, so that any handle_info/2 the user
      # writes is matched after this one. Being quote-generated, this clause carries
      # `generated: true` metadata, which is why it raises neither the clause-grouping
      # warning nor the missing-@impl warning. `__verified_pubsub_dispatch__/4` is a
      # forward reference, defined at @before_compile.
      def handle_info(%VerifiedPubSub.Message{} = message, state) do
        __verified_pubsub_dispatch__(message.topic, message.event, message, state)
      end
    end
  end

  @doc """
  Handles one event on one topic.

  `pattern` matches the message `payload`, unless it is syntactically a
  `%VerifiedPubSub.Message{}` pattern, in which case it matches the whole message and
  so can match on `params`.
  """
  defmacro handle_message(topic, event, pattern, state, do: body) do
    Module.put_attribute(__CALLER__.module, :verified_pubsub_clauses, %{
      topic: literal_atom!(topic, :topic),
      event: literal_atom!(event, :event),
      pattern: pattern,
      state: state,
      body: body,
      whole_message?: message_pattern?(pattern, __CALLER__),
      line: __CALLER__.line
    })

    nil
  end

  @doc """
  Declares that this module knowingly does nothing with an event, or a list of them.

      ignore_message :campaigns, :deleted
      ignore_message :campaigns, [:updated, :deleted]

  Satisfies exhaustiveness without a handler. The generated clause returns
  `{:noreply, state}`, which is correct for both GenServer and LiveView.
  """
  defmacro ignore_message(topic, event_or_events) do
    topic = literal_atom!(topic, :topic)

    events =
      case event_or_events do
        list when is_list(list) ->
          if list == [] do
            raise ArgumentError,
                  "ignore_message #{inspect(topic)}, [] ignores nothing. " <>
                    "Pass at least one event, or remove the call."
          end

          Enum.map(list, &literal_atom!(&1, :event))

        event ->
          [literal_atom!(event, :event)]
      end

    Enum.each(events, fn event ->
      Module.put_attribute(__CALLER__.module, :verified_pubsub_ignored, {topic, event})
    end)

    nil
  end

  defp literal_atom!(ast, _role) when is_atom(ast), do: ast

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
    Macro.expand(alias_ast, env) == VerifiedPubSub.Message
  end

  defp message_pattern?(_, _), do: false

  defmacro __before_compile__(env) do
    clauses = env.module |> Module.get_attribute(:verified_pubsub_clauses) |> Enum.reverse()
    ignored = env.module |> Module.get_attribute(:verified_pubsub_ignored) |> Enum.reverse()

    VerifiedPubSub.Subscriber.Verify.run!(env, clauses, ignored)

    dispatch = Enum.map(clauses, &dispatch_clause/1) ++ Enum.map(ignored, &ignored_clause/1)

    quote do
      (unquote_splicing(no_handlers_fallback(dispatch) ++ dispatch))
    end
  end

  # The generated handle_info/2 calls __verified_pubsub_dispatch__/4 unconditionally, so
  # the function must exist even when every event was dismissed with `on_missing:
  # :ignore` and no clause was written. Raising is right here: a verified message
  # arrived that this module claimed no interest in.
  defp no_handlers_fallback([]) do
    [
      quote do
        defp __verified_pubsub_dispatch__(topic, event, _message, _state) do
          raise """
          \#{inspect(__MODULE__)} received \#{inspect(event)} on \#{inspect(topic)} but \
          defines no handle_message or ignore_message clauses.
          """
        end
      end
    ]
  end

  defp no_handlers_fallback(_dispatch), do: []

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
             %VerifiedPubSub.Message{payload: unquote(clause.pattern)},
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
