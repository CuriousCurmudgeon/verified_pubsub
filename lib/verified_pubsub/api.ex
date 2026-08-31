defmodule VerifiedPubSub.Api do
  @moduledoc """
  The call-site API: `subscribe/2`, `unsubscribe/2`, `topic/2`, `broadcast/4`,
  `broadcast!/4`, `broadcast_from/5` and `broadcast_from!/5`.

  Imported by `use VerifiedPubSub, registry: MyApp.Topics`, and by
  `use VerifiedPubSub.Subscriber`:

      broadcast!(:campaigns, :created, %{account_id: id}, payload)

  These are **macros**, so the topic and event must be literal atoms. That is what allows
  an unknown topic or event to be a `CompileError` naming the valid alternatives. Params
  may be built at runtime; a literal params map is checked at compile time, and a dynamic
  one raises `KeyError` from `Map.fetch!/2` when a key is missing.

  The costs: every calling module needs the import, and macros cannot be piped into,
  captured with `&`, or called via `apply/3`.
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
    validate_literal_payload!(caller, registry, topic_struct.name, event, payload)

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

      # Bound once, so the payload expression is not evaluated twice, and validated
      # before anything is sent.
      vp_payload =
        VerifiedPubSub.Payload.validate!(
          unquote(registry),
          unquote(topic_struct.name),
          unquote(event),
          unquote(payload)
        )

      vp_message = %VerifiedPubSub.Message{
        registry: unquote(registry),
        topic: unquote(topic_struct.name),
        event: unquote(event),
        params: vp_params,
        payload: vp_payload
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
    if literal_keys(pairs) do
      keys = literal_keys(pairs)
      expected = topic_struct.params
      missing = expected -- keys
      unexpected = keys -- expected

      if missing == [] and unexpected == [] do
        :ok
      else
        raise_compile_error(caller, params_message(topic_struct, missing, unexpected))
      end
    else
      :ok
    end
  end

  defp validate_params!(_caller, _topic_struct, _params), do: :ok

  # A payload built at runtime can only be checked by VerifiedPubSub.Payload when the
  # broadcast runs. A literal map, though, is fully known here, so the same checks run at
  # compile time and fail the build instead.
  defp validate_literal_payload!(caller, registry, topic, event, {:%{}, _, pairs})
       when is_list(pairs) do
    with keys when is_list(keys) <- literal_keys(pairs) do
      fields = Info.fields(registry, topic, event)
      declared = Enum.map(fields, & &1.name)
      required = fields |> Enum.filter(& &1.required) |> Enum.map(& &1.name)

      missing = required -- keys
      unexpected = keys -- declared
      type_problems = literal_type_problems(fields, pairs)

      if missing == [] and unexpected == [] and type_problems == [] do
        :ok
      else
        raise_compile_error(
          caller,
          literal_payload_message(topic, event, fields, missing, unexpected, type_problems)
        )
      end
    end

    :ok
  end

  defp validate_literal_payload!(_caller, _registry, _topic, _event, _payload), do: :ok

  # Only values that are literals in the AST can be judged. A variable or a call is left
  # to the runtime check.
  defp literal_type_problems(fields, pairs) do
    for {key, value} <- pairs,
        field = Enum.find(fields, &(&1.name == key)),
        literal_value?(value),
        not VerifiedPubSub.Payload.valid_field?(field, value) do
      {key, field.type, value}
    end
  end

  defp literal_value?(value) do
    is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or
      is_nil(value)
  end

  defp literal_payload_message(topic, event, fields, missing, unexpected, type_problems) do
    detail =
      [
        if(missing != [], do: "  missing required: #{inspect(missing)}"),
        if(unexpected != [], do: "  unexpected: #{inspect(unexpected)}")
      ]
      |> Enum.concat(
        Enum.map(type_problems, fn {key, type, value} ->
          "  #{inspect(key)} is declared as #{inspect(type)}, got: #{inspect(value)}"
        end)
      )
      |> Enum.reject(&is_nil/1)
      |> Enum.join("\n")

    declared =
      Enum.map_join(fields, "\n", fn field ->
        "    field #{inspect(field.name)}, #{inspect(field.type)}" <>
          if(field.required, do: "", else: ", required: false")
      end)

    """
    invalid payload for #{inspect(topic)} #{inspect(event)}.

    #{detail}

    Declared:

    #{declared}
    """
  end

  # The keys of a literal map, or nil when the AST is not one we can read statically.
  # `%{base | k: v}` arrives as a single three-element `{:|, meta, [_, _]}` tuple rather
  # than key/value pairs, and would otherwise be misread as a map with the key `:|`.
  defp literal_keys(pairs) do
    if Enum.all?(pairs, &match?({key, _value} when is_atom(key), &1)) do
      Enum.map(pairs, &elem(&1, 0))
    end
  end

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
