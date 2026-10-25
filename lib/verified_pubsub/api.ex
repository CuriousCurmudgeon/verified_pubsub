defmodule VerifiedPubSub.Api do
  @moduledoc """
  The call-site API: `subscribe/2`, `unsubscribe/2`, `topic/2`, `broadcast/4`,
  `broadcast!/4`, `broadcast_from/5` and `broadcast_from!/5`.

  `use VerifiedPubSub, manifest: MyApp.Topics` imports these macros, and so does
  `use VerifiedPubSub.Subscriber`:

      broadcast!(:pets, %{owner_id: id}, :adopted, payload)

  ## Argument order

  In every macro here, the params come immediately after the topic. The params only fill
  in the segments of the topic, so `{topic, params}` is the address and `{event, payload}`
  is the message. `Phoenix.PubSub.broadcast/3` has the same order: the address, then the
  message. Also, the two atoms are never next to each other, so a call site cannot swap
  them without notice.

  ## Why macros rather than functions

  These are **macros**, so the topic and the event must be literal atoms. Because of this,
  an unknown topic or event is a `CompileError` that names the valid choices. A call site
  can build the params at runtime. The macro checks a literal params map at compile time.
  A params map built at runtime raises `KeyError` from `Map.fetch!/2` when a key is
  missing.

  A plain function that takes atoms gets no check at compile time. Elixir's type inference
  does not narrow across clause heads on a remote call. So a `broadcast/4` defined as

      def broadcast(:pets, %{owner_id: id}, :adopted, payload), do: ...

  gives **no diagnostic at all** for `broadcast(:pets, params, :adoptd, ...)`. We checked
  this on Elixir 1.20.4. The call fails at runtime instead. A macro can find the topic and
  the event in the manifest while your code compiles.

  The macros have these costs:

    * each module that calls them needs the import
    * a pipe cannot pass a value into them
    * `&` cannot capture them
    * `apply/3` cannot call them

  ## Broadcasts that skip the sender

  `broadcast_from/5` and `broadcast_from!/5` work like `Phoenix.PubSub.broadcast_from/4`.
  `from` comes first for the same reason that it does there:

      broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, payload)

  A common use is a LiveView that writes to a topic and also subscribes to it. Without
  these macros, the LiveView applies its own change twice.

  `from` is the pid that you pass, so `self()` is the process that makes the call. When a
  context function, a `Task`, or an Oban job makes the broadcast, `self()` is *that*
  process, not the process that started the request. In that case, pass the pid of the
  process that started the request.
  """

  alias VerifiedPubSub.{Info, Topic}

  @doc "Subscribes the current process to a topic."
  defmacro subscribe(topic, params \\ quote(do: %{})) do
    {manifest, topic_struct} = resolve!(__CALLER__, topic)
    validate_params!(__CALLER__, topic_struct, params)
    Module.put_attribute(__CALLER__.module, :verified_pubsub_subscriptions, topic_struct.name)

    quote do
      unquote(bind_topic(manifest, topic_struct, params))
      Phoenix.PubSub.subscribe(unquote(manifest).__verified_pubsub_name__(), vp_topic)
    end
  end

  @doc "Unsubscribes the current process from a topic."
  defmacro unsubscribe(topic, params \\ quote(do: %{})) do
    {manifest, topic_struct} = resolve!(__CALLER__, topic)
    validate_params!(__CALLER__, topic_struct, params)

    quote do
      unquote(bind_topic(manifest, topic_struct, params))
      Phoenix.PubSub.unsubscribe(unquote(manifest).__verified_pubsub_name__(), vp_topic)
    end
  end

  @doc "Returns the wire topic."
  defmacro topic(topic, params \\ quote(do: %{})) do
    {manifest, topic_struct} = resolve!(__CALLER__, topic)
    validate_params!(__CALLER__, topic_struct, params)

    quote do
      unquote(bind_topic(manifest, topic_struct, params))
      vp_topic
    end
  end

  @doc """
  Broadcasts an event on a topic that takes no params.

  It is the same as `broadcast(topic, %{}, event, payload)`. When the topic takes params,
  the call is a compile error. `subscribe/1` works the same way.
  """
  defmacro broadcast(topic, event, payload) do
    build(__CALLER__, topic, no_params(), event, payload, nil, false)
  end

  @doc "Broadcasts an event on a topic."
  defmacro broadcast(topic, params, event, payload) do
    build(__CALLER__, topic, params, event, payload, nil, false)
  end

  @doc "Broadcasts an event on a topic that takes no params, and raises on failure."
  defmacro broadcast!(topic, event, payload) do
    build(__CALLER__, topic, no_params(), event, payload, nil, true)
  end

  @doc "Broadcasts an event on a topic, and raises on failure."
  defmacro broadcast!(topic, params, event, payload) do
    build(__CALLER__, topic, params, event, payload, nil, true)
  end

  @doc "Broadcasts on a topic that takes no params, to every subscribed process except `from`."
  defmacro broadcast_from(from, topic, event, payload) do
    build(__CALLER__, topic, no_params(), event, payload, from, false)
  end

  @doc "Broadcasts to every subscribed process except `from`."
  defmacro broadcast_from(from, topic, params, event, payload) do
    build(__CALLER__, topic, params, event, payload, from, false)
  end

  @doc """
  Broadcasts on a topic that takes no params, to every subscribed process except `from`,
  and raises on failure.
  """
  defmacro broadcast_from!(from, topic, event, payload) do
    build(__CALLER__, topic, no_params(), event, payload, from, true)
  end

  @doc "Broadcasts to every subscribed process except `from`, and raises on failure."
  defmacro broadcast_from!(from, topic, params, event, payload) do
    build(__CALLER__, topic, params, event, payload, from, true)
  end

  defp resolve!(caller, topic_ast) do
    manifest = manifest!(caller)
    topic = literal_atom!(caller, topic_ast, "topic")

    case Info.topic(manifest, topic) do
      {:ok, topic_struct} ->
        {manifest, topic_struct}

      :error ->
        raise_compile_error(caller, """
        unknown topic #{inspect(topic)} in #{inspect(manifest)}.

        Declared topics:
        #{Topic.listing(Info.topics(manifest))}
        """)
    end
  end

  defp manifest!(caller) do
    case Module.get_attribute(caller.module, :verified_pubsub_manifest) do
      nil ->
        raise_compile_error(caller, """
        no manifest is in scope.

        Add `use VerifiedPubSub, manifest: MyApp.Topics` to #{inspect(caller.module)}.
        """)

      manifest ->
        manifest
    end
  end

  defp raise_compile_error(caller, description) do
    raise CompileError, file: caller.file, line: caller.line, description: description
  end

  defp literal_atom!(caller, ast, role) when is_atom(ast) and not is_nil(ast) do
    _ = {caller, role}
    ast
  end

  defp literal_atom!(caller, ast, role) do
    raise_compile_error(caller, """
    expected a literal atom for #{role}, got: #{Macro.to_string(ast)}

    These are macros, so the #{role} must be known at compile time in order to be
    verified. For a value chosen at runtime, look it up in the manifest yourself with
    VerifiedPubSub.Info and call Phoenix.PubSub directly.
    """)
  end

  # Only a literal map has keys that this function can check at compile time. For any
  # other expression, such as a variable or a function call, `Map.fetch!/2` checks at
  # runtime. It raises KeyError and names the missing key.
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

  # Returns the keys of a literal map, or nil when the AST is not a map that this function
  # can read at compile time. `%{base | k: v}` arrives as one three-element tuple,
  # `{:|, meta, [_, _]}`, not as key/value pairs. Without this check, it reads as a map
  # with the key `:|`.
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

    declared_at = Topic.declared_at(inspect(topic_struct.name), topic_struct)

    """
    wrong params for topic #{inspect(topic_struct.name)}.

    #{detail}

    #{inspect(topic_struct.name)} is #{Topic.pattern(topic_struct.segments)}, so it takes exactly #{inspect(topic_struct.params)}.#{declared_at}
    """
  end

  # Binds `vp_params` and `vp_topic`. `Macro.var/2` gives the vars the context of this
  # module. The `quote` blocks above write `vp_params` and `vp_topic` literally, and
  # hygiene gives them the same context, so the names match.
  defp bind_topic(manifest, topic_struct, params) do
    pvar = Macro.var(:vp_params, __MODULE__)
    tvar = Macro.var(:vp_topic, __MODULE__)

    interpolation =
      topic_struct.segments
      |> Enum.map(fn
        literal when is_binary(literal) ->
          literal

        param ->
          quote do
            VerifiedPubSub.Topic.segment!(
              unquote(manifest),
              unquote(topic_struct.name),
              unquote(param),
              Map.fetch!(unquote(pvar), unquote(param))
            )
          end
      end)
      |> Enum.intersperse(VerifiedPubSub.Topic.separator())
      |> Enum.reduce(&quote(do: unquote(&2) <> unquote(&1)))

    quote do
      unquote(pvar) = unquote(params)
      unquote(tvar) = unquote(interpolation)
      _ = unquote(tvar)
    end
  end

  defp build(caller, topic, params, event, payload, from, bang?) do
    {manifest, topic_struct} = resolve!(caller, topic)
    event = validate_event!(caller, manifest, topic_struct, event)
    validate_params!(caller, topic_struct, params)
    validate_literal_payload!(caller, topic_struct, event, payload)
    Module.put_attribute(caller.module, :verified_pubsub_broadcasts, {topic_struct.name, event})

    call =
      if from do
        quote do
          Phoenix.PubSub.broadcast_from(
            unquote(manifest).__verified_pubsub_name__(),
            unquote(from),
            vp_topic,
            vp_message
          )
        end
      else
        quote do
          Phoenix.PubSub.broadcast(
            unquote(manifest).__verified_pubsub_name__(),
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
            vp_topic,
            unquote(event)
          )
        end
      else
        call
      end

    quote do
      unquote(bind_topic(manifest, topic_struct, params))

      # This binds the payload once, so that its expression runs once. It validates the
      # payload before the broadcast.
      vp_payload =
        VerifiedPubSub.Payload.validate!(
          unquote(manifest),
          unquote(topic_struct.name),
          unquote(event),
          unquote(payload)
        )

      vp_message = %VerifiedPubSub.Message{
        manifest: unquote(manifest),
        topic: unquote(topic_struct.name),
        event: unquote(event),
        params: vp_params,
        payload: vp_payload
      }

      unquote(call)
    end
  end

  defp validate_event!(caller, manifest, topic_struct, event_ast) do
    event = literal_atom!(caller, event_ast, "event")
    declared = Enum.map(topic_struct.messages, & &1.name)

    if event in declared do
      event
    else
      elsewhere =
        manifest
        |> Info.topics()
        |> Enum.filter(&(event in Enum.map(&1.messages, fn m -> m.name end)))
        |> Enum.map(& &1.name)

      hint =
        if elsewhere == [] do
          ""
        else
          "\n#{inspect(event)} is declared on #{inspect(elsewhere)}, not #{inspect(topic_struct.name)}."
        end

      declared_at = Topic.declared_at(inspect(topic_struct.name), topic_struct)

      raise_compile_error(caller, """
      unknown event #{inspect(event)} on topic #{Topic.describe(topic_struct)}.

      Declared events: #{inspect(declared)}#{hint}#{declared_at}
      """)
    end
  end

  # VerifiedPubSub.Payload checks a payload built at runtime when the broadcast runs. A
  # literal map is fully known at compile time, so this function runs the same checks
  # here, and a failure stops the build.
  defp validate_literal_payload!(caller, topic_struct, event, {:%{}, _, pairs})
       when is_list(pairs) do
    with keys when is_list(keys) <- literal_keys(pairs) do
      message = Enum.find(topic_struct.messages, &(&1.name == event))
      fields = message.fields
      declared = Enum.map(fields, & &1.name)

      required =
        fields
        |> Enum.filter(& &1.required)
        |> Enum.map(& &1.name)

      missing = required -- keys
      unexpected = keys -- declared
      type_problems = literal_type_problems(fields, pairs)

      if missing == [] and unexpected == [] and type_problems == [] do
        :ok
      else
        raise_compile_error(
          caller,
          literal_payload_message(topic_struct.name, message, missing, unexpected, type_problems)
        )
      end
    end

    :ok
  end

  defp validate_literal_payload!(_caller, _topic_struct, _event, _payload), do: :ok

  # This function can check only a value that is a literal in the AST. The runtime check
  # handles a variable or a call.
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

  defp literal_payload_message(topic, message, missing, unexpected, type_problems) do
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
      Enum.map_join(message.fields, "\n", fn field ->
        "    field #{inspect(field.name)}, #{inspect(field.type)}" <>
          if(field.required, do: "", else: ", required: false")
      end)

    """
    invalid payload for #{inspect(topic)} #{inspect(message.name)}.

    #{detail}

    #{declared_heading(message)}:

    #{declared}
    """
  end

  defp declared_heading(message) do
    case Topic.location(message) do
      nil -> "Declared"
      location -> "Declared at #{location}"
    end
  end

  # The AST of an empty params map, for the arities with no params argument. A default
  # argument shows `params \\ %{}` in the middle of the signature in the docs. This
  # function gives the shorter arity a signature of its own instead.
  defp no_params, do: {:%{}, [], []}
end
