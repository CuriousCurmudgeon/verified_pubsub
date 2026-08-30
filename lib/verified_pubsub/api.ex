defmodule VerifiedPubSub.Api do
  @moduledoc """
  Atom-first macros: an experimental alternative to the generated functions.

  Instead of `MyApp.Topics.broadcast_campaigns_created!(params, payload)`, the topic and
  event are ordinary arguments:

      use VerifiedPubSub, registry: MyApp.Topics

      broadcast!(:campaigns, :created, %{account_id: id}, payload)

  Because these are macros, the topic and event must be **literal atoms**, which is what
  lets a typo be a hard `CompileError` rather than the compile *warning* an undefined
  generated function produces.

  The trade is that every calling module needs `use VerifiedPubSub, registry: ...`, and
  macros cannot be piped into, captured with `&`, or called via `apply/3`.
  """

  alias VerifiedPubSub.Info

  @doc "Subscribes the calling process to a topic."
  defmacro subscribe(topic, params \\ quote(do: %{})) do
    {registry, topic_struct} = resolve!(__CALLER__, topic)
    validate_params!(__CALLER__, topic_struct, params)

    quote do
      unquote(bind_topic(registry, topic_struct, params))
      Phoenix.PubSub.subscribe(unquote(registry).__verified_pubsub_name__(), vp_topic)
    end
  end

  @doc "Unsubscribes the calling process from a topic."
  defmacro unsubscribe(topic, params \\ quote(do: %{})) do
    {registry, topic_struct} = resolve!(__CALLER__, topic)
    validate_params!(__CALLER__, topic_struct, params)

    quote do
      unquote(bind_topic(registry, topic_struct, params))
      Phoenix.PubSub.unsubscribe(unquote(registry).__verified_pubsub_name__(), vp_topic)
    end
  end

  @doc "Returns the wire topic string."
  defmacro topic(topic, params \\ quote(do: %{})) do
    {registry, topic_struct} = resolve!(__CALLER__, topic)
    validate_params!(__CALLER__, topic_struct, params)

    quote do
      unquote(bind_topic(registry, topic_struct, params))
      vp_topic
    end
  end

  @doc "Broadcasts an event on a topic."
  defmacro broadcast(topic, event, params, payload) do
    build(__CALLER__, topic, event, params, payload, nil, false)
  end

  @doc "Broadcasts an event on a topic, raising on failure."
  defmacro broadcast!(topic, event, params, payload) do
    build(__CALLER__, topic, event, params, payload, nil, true)
  end

  @doc "Broadcasts to every subscriber except `from`."
  defmacro broadcast_from(from, topic, event, params, payload) do
    build(__CALLER__, topic, event, params, payload, from, false)
  end

  @doc "Broadcasts to every subscriber except `from`, raising on failure."
  defmacro broadcast_from!(from, topic, event, params, payload) do
    build(__CALLER__, topic, event, params, payload, from, true)
  end

  # -- expansion helpers -------------------------------------------------------

  defp build(caller, topic, event, params, payload, from, bang?) do
    {registry, topic_struct} = resolve!(caller, topic)
    event = validate_event!(caller, registry, topic_struct, event)
    validate_params!(caller, topic_struct, params)

    call =
      if from do
        quote do
          Phoenix.PubSub.broadcast_from(
            unquote(registry).__verified_pubsub_name__(),
            unquote(from),
            vp_topic,
            vp_message
          )
        end
      else
        quote do
          Phoenix.PubSub.broadcast(
            unquote(registry).__verified_pubsub_name__(),
            vp_topic,
            vp_message
          )
        end
      end

    call =
      if bang? do
        quote do
          VerifiedPubSub.Broadcast.bang!(
            unquote(call),
            unquote(topic_struct.name),
            unquote(event)
          )
        end
      else
        call
      end

    quote do
      unquote(bind_topic(registry, topic_struct, params))

      vp_message = %VerifiedPubSub.Message{
        registry: unquote(registry),
        topic: unquote(topic_struct.name),
        event: unquote(event),
        params: vp_params,
        payload: unquote(payload)
      }

      unquote(call)
    end
  end

  # Binds `vp_params` and `vp_topic`. The vars are created with this module's context so
  # they match the `vp_params` / `vp_topic` written literally inside the `quote` blocks
  # above, which hygiene also stamps with this module.
  defp bind_topic(_registry, topic_struct, params) do
    pvar = Macro.var(:vp_params, __MODULE__)
    tvar = Macro.var(:vp_topic, __MODULE__)
    literals = String.split(topic_struct.pattern, ~r/%\{[^}]*\}/)

    interpolation =
      topic_struct.params
      |> Enum.zip(tl(literals))
      |> Enum.reduce(hd(literals), fn {param, literal}, acc ->
        quote do
          unquote(acc) <>
            to_string(Map.fetch!(unquote(pvar), unquote(param))) <>
            unquote(literal)
        end
      end)

    quote do
      unquote(pvar) = unquote(params)
      unquote(tvar) = unquote(interpolation)
      _ = unquote(tvar)
    end
  end

  # -- compile-time validation -------------------------------------------------

  defp resolve!(caller, topic_ast) do
    registry = registry!(caller)
    topic = literal_atom!(caller, topic_ast, "topic")

    case Info.topic(registry, topic) do
      {:ok, topic_struct} ->
        {registry, topic_struct}

      :error ->
        known = registry |> Info.topics() |> Enum.map(& &1.name) |> Enum.sort()

        raise_compile_error(caller, """
        unknown topic #{inspect(topic)} in #{inspect(registry)}.

        Declared topics: #{inspect(known)}
        """)
    end
  end

  defp validate_event!(caller, registry, topic_struct, event_ast) do
    event = literal_atom!(caller, event_ast, "event")
    declared = Enum.map(topic_struct.messages, & &1.name)

    if event in declared do
      event
    else
      elsewhere =
        registry
        |> Info.topics()
        |> Enum.filter(&(event in Enum.map(&1.messages, fn m -> m.name end)))
        |> Enum.map(& &1.name)

      hint =
        if elsewhere == [] do
          ""
        else
          "\n#{inspect(event)} is declared on #{inspect(elsewhere)}, not #{inspect(topic_struct.name)}."
        end

      raise_compile_error(caller, """
      unknown event #{inspect(event)} on topic #{inspect(topic_struct.name)}.

      Declared events: #{inspect(declared)}#{hint}
      """)
    end
  end

  # Only a literal map can be checked at compile time. Anything else (a variable, a
  # function call) is left to `Map.fetch!/2` at runtime, which raises KeyError naming the
  # missing key.
  defp validate_params!(caller, topic_struct, {:%{}, _, pairs}) when is_list(pairs) do
    keys = Enum.map(pairs, &elem(&1, 0))

    if Enum.all?(keys, &is_atom/1) do
      expected = topic_struct.params
      missing = expected -- keys
      unexpected = keys -- expected

      cond do
        missing == [] and unexpected == [] ->
          :ok

        true ->
          raise_compile_error(caller, params_message(topic_struct, missing, unexpected))
      end
    else
      :ok
    end
  end

  defp validate_params!(_caller, _topic_struct, _params), do: :ok

  defp params_message(topic_struct, missing, unexpected) do
    detail =
      [
        if(missing != [], do: "  missing: #{inspect(missing)}"),
        if(unexpected != [], do: "  unexpected: #{inspect(unexpected)}")
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join("\n")

    """
    wrong params for topic #{inspect(topic_struct.name)}.

    #{detail}

    #{inspect(topic_struct.name)} is #{inspect(topic_struct.pattern)}, so it takes exactly #{inspect(topic_struct.params)}.
    """
  end

  defp literal_atom!(caller, ast, role) when is_atom(ast) and not is_nil(ast) do
    _ = {caller, role}
    ast
  end

  defp literal_atom!(caller, ast, role) do
    raise_compile_error(caller, """
    expected a literal atom for #{role}, got: #{Macro.to_string(ast)}

    These are macros, so the #{role} must be known at compile time in order to be
    verified. For a value chosen at runtime, look it up in the registry yourself with
    VerifiedPubSub.Info and call Phoenix.PubSub directly.
    """)
  end

  defp registry!(caller) do
    case Module.get_attribute(caller.module, :verified_pubsub_registry) do
      nil ->
        raise_compile_error(caller, """
        no registry is in scope.

        Add `use VerifiedPubSub, registry: MyApp.Topics` to #{inspect(caller.module)}.
        """)

      registry ->
        registry
    end
  end

  defp raise_compile_error(caller, description) do
    raise CompileError, file: caller.file, line: caller.line, description: description
  end
end
