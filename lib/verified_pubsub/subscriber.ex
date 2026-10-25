defmodule VerifiedPubSub.Subscriber do
  @moduledoc """
  Declares that a module subscribes to topics of a manifest, and defines its
  handlers.

      defmodule MyAppWeb.PetsLive do
        use MyAppWeb, :live_view
        use VerifiedPubSub.Subscriber, manifest: MyApp.Topics

        def mount(_params, _session, socket) do
          if connected?(socket), do: subscribe(:pets, %{owner_id: socket.assigns.id})
          {:ok, socket}
        end

        handle_message :pets, :adopted, payload, socket do
          {:noreply, stream_insert(socket, :pets, payload)}
        end

        ignore_message :pets, :rehomed
      end

  The `handle_message/5` and `ignore_message/2` calls of the module set the topics that it
  subscribes to. You keep no separate list of topics. The module must handle or dismiss
  each event that those topics declare, or the module does not compile.

  `use` also imports `VerifiedPubSub.Api`. The module can then call `subscribe/2`,
  `broadcast!/4`, and the other macros without a separate
  `use VerifiedPubSub, manifest: ...`.

  ## Docs

  With `docs: true`, the module gets a `## PubSub` section at the end of its `@moduledoc`.
  The section lists each topic that the module handles, with the events of its
  `handle_message/5` calls. An event of `ignore_message/2` does not appear. The section
  also lists the broadcasts of the module, as `VerifiedPubSub` describes.

  ```markdown
  ## PubSub

    * Subscribes to `"owners:<owner_id>:pets"` for `:adopted`.
  ```

  ## Generated code

  `use` generates two `handle_info/2` clauses at the `use` site. The first clause matches
  a `VerifiedPubSub.Message` from the manifest of *this* module. It passes the message to
  private `__verified_pubsub_dispatch__/4` clauses, which the module defines at
  `@before_compile`. The second clause matches a message from any other manifest and
  raises `VerifiedPubSub.ManifestMismatchError`. See "Messages from another manifest"
  below.

  `handle_message/5` therefore generates no code of its own. It adds the clause to a
  module attribute. The coverage check needs every clause, and the module has every
  clause only at `@before_compile`. The attribute does *not* exist to avoid the
  clause-grouping warning of Elixir, as an earlier version of this documentation said. A
  clause that a macro generates carries `generated: true` metadata, and Elixir gives no
  such warning for it.

  The `handle_info/2` clauses come from the `use` site, not from `@before_compile`, so
  your own `handle_info/2` clauses still work. Elixir tries your clauses *after* the
  generated clauses. A catch-all clause of yours thus receives each message that is not
  verified, and verified messages still reach the generated clauses.

  ## Messages from another manifest

  An application can declare more than one manifest. Two manifests can produce the same
  wire topic when they share a `:pubsub` and declare topics that build the same string.
  The disjointness check works only *within* one manifest, because a manifest cannot see
  the other manifests while it compiles.

  In that case, `Phoenix.PubSub` delivers the message, and its `topic` and `event` atoms
  can match a clause here. But its payload follows the field declarations of the
  *sender*. The compiler never checked that payload against the handler. Thus the
  generated clause raises `VerifiedPubSub.ManifestMismatchError`, and the handler does
  not run.

  That clause comes before each `handle_info/2` clause that you write, on purpose. A
  catch-all clause of yours thus does **not** receive a message from another manifest. A
  catch-all clause is the documented way to ignore unrelated messages. If it received
  this message, nothing would ever report the collision.

  For full isolation, give each manifest its own `Phoenix.PubSub`. Each instance has its
  own registry, so a message never goes from one instance to the other:

      children = [
        Supervisor.child_spec({Phoenix.PubSub, name: Pets.PubSub}, id: :pets_pubsub),
        Supervisor.child_spec({Phoenix.PubSub, name: Treats.PubSub}, id: :treats_pubsub)
      ]

  Which modules can bind which manifest is a separate question of dependency boundaries.
  A manifest is a plain module, so `mix xref` or the `boundary` package can enforce
  those boundaries better than this library can.

  ## Messages this module does not expect

  A module that defines a `handle_info/2` clause loses the default clause that
  `use GenServer` and `use Phoenix.LiveView` add. An unexpected message then raises
  `FunctionClauseError`, and nothing logs it. Each GenServer with its own
  `handle_info/2` has this behavior. This module cannot prevent it: Elixir 1.20 made
  `super/2` for GenServer callbacks an error, so no code can call the default body.

  When your process receives messages that are not verified, add your own catch-all
  clause:

      handle_message :pets, :adopted, payload, state do
        {:noreply, state}
      end

      # Matched after the generated clauses, so verified messages still reach them.
      def handle_info(_other, state), do: {:noreply, state}
  """

  @options [:docs, :manifest]

  defmacro __using__(opts) do
    manifest =
      opts
      |> Keyword.fetch!(:manifest)
      |> Macro.expand(__CALLER__)

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

    module = __CALLER__.module

    # A second `use` would generate the handle_info/2 clauses and the @before_compile
    # dispatch twice, and the second copy would never match.
    if Module.has_attribute?(module, :verified_pubsub_clauses) do
      raise ArgumentError,
            "#{inspect(module)} already uses VerifiedPubSub.Subscriber. A module " <>
              "subscribes through exactly one manifest, so use it once."
    end

    VerifiedPubSub.bind_manifest!(module, manifest)
    VerifiedPubSub.bind_docs!(module, Keyword.get(opts, :docs))

    # Register these attributes here, during expansion, not inside the quote below.
    # Elixir expands the macros of a module body before the runtime calls of the body
    # run. A `Module.register_attribute` call in the quote thus has no effect when
    # `handle_message` expands. Each put would overwrite the last one, and the later
    # registration would reset the attribute to []. A registration here makes the
    # attribute accumulate before any `handle_message` in the body expands.
    Module.register_attribute(module, :verified_pubsub_clauses, accumulate: true)
    Module.register_attribute(module, :verified_pubsub_ignored, accumulate: true)

    quote do
      import VerifiedPubSub.Api

      import VerifiedPubSub.Subscriber,
        only: [handle_message: 5, handle_message: 6, ignore_message: 2]

      @before_compile VerifiedPubSub.Subscriber

      # The quote defines these clauses here, not at @before_compile, so that Elixir
      # tries each handle_info/2 clause of the user after them. A quote generates them,
      # so they carry `generated: true` metadata. Thus they cause neither the
      # clause-grouping warning nor the missing-@impl warning.
      # `__verified_pubsub_dispatch__/4` is a forward reference to @before_compile.
      def handle_info(
            %VerifiedPubSub.Message{manifest: unquote(manifest)} = message,
            state
          ) do
        __verified_pubsub_dispatch__(message.topic, message.event, message, state)
      end

      # A message from another manifest matches no clause that this module verified.
      # Its topic and event atoms can be equal to ours, but its payload follows the
      # declarations of the sender. This clause comes before each handle_info clause of
      # the user, on purpose. A catch-all clause is the documented way to ignore
      # unrelated messages. If it received a message from another manifest, nothing
      # would ever report the collision.
      def handle_info(%VerifiedPubSub.Message{} = message, _state) do
        raise VerifiedPubSub.ManifestMismatchError,
          subscriber: __MODULE__,
          expected: unquote(manifest),
          got: message.manifest,
          topic: message.topic,
          event: message.event,
          params: message.params
      end
    end
  end

  @doc """
  Handles one event, or a list of events, on one topic.

  `pattern` matches the `payload` of the message. When `pattern` is a
  `%VerifiedPubSub.Message{}` pattern in the source, it matches the whole message
  instead, so it can match on `params` and `event`. A payload pattern must fit each event
  in the list.

  ## Usage

      handle_message :pets, :adopted, payload, socket do
        {:noreply, stream_insert(socket, :pets, payload)}
      end

      handle_message :pets, [:adopted, :renamed], %{id: id}, socket do
        {:noreply, assign(socket, :last_changed, id)}
      end
  """
  defmacro handle_message(topic, event, pattern, state, do: body) do
    put_clause!(__CALLER__, topic, nil, event, pattern, state, body)
  end

  @doc """
  Handles one event, or a list of events, on one topic, and matches the params of the
  topic.

  The first arguments are the arguments of `broadcast!/4`, in the same order, but here
  they **match** and do not build. Unlike a broadcast, the pattern can name a subset of
  the params. A param that the topic does not declare is a compile error, because that
  pattern can never match.

  ## Usage

      broadcast!     :pets, %{owner_id: id},    :adopted, payload
      handle_message :pets, %{owner_id: owner}, :adopted, payload, socket

  The clause binds `owner` from the params, without a match on the whole message. A
  literal limits the clause to that value:

      handle_message :pets, %{owner_id: "7"}, :adopted, payload, socket
  """
  defmacro handle_message(topic, params, event, pattern, state, do: body) do
    put_clause!(__CALLER__, topic, params, event, pattern, state, body)
  end

  @doc """
  Declares that this module does nothing with an event, or with a list of events.

      ignore_message :pets, :rehomed
      ignore_message :pets, [:renamed, :rehomed]

  The call satisfies the coverage check without a handler. The generated clause returns
  `{:noreply, state}`, which is correct for both a GenServer and a LiveView.
  """
  defmacro ignore_message(topic, event_or_events) do
    topic = literal_atom!(topic, :topic)

    Enum.each(literal_events!(:ignore_message, topic, event_or_events), fn event ->
      Module.put_attribute(__CALLER__.module, :verified_pubsub_ignored, {topic, event})
    end)

    nil
  end

  defmacro __before_compile__(env) do
    clauses =
      env.module
      |> Module.get_attribute(:verified_pubsub_clauses)
      |> Enum.reverse()

    ignored =
      env.module
      |> Module.get_attribute(:verified_pubsub_ignored)
      |> Enum.reverse()

    VerifiedPubSub.Subscriber.Verify.run!(env, clauses, ignored)

    dispatch = Enum.map(clauses, &dispatch_clause/1) ++ Enum.map(ignored, &ignored_clause/1)

    # The moduledoc goes last. A `broadcast!` in a `handle_message` body expands only inside
    # the dispatch clauses above, so an earlier write would not see it.
    quote do
      (unquote_splicing(no_handlers_fallback(dispatch) ++ dispatch))
      VerifiedPubSub.Docs.put_moduledoc(__MODULE__, unquote(env.line))
    end
  end

  defp dispatch_clause(%{whole_message?: true} = clause) do
    quote line: clause.line do
      defp __verified_pubsub_dispatch__(
             unquote(clause.topic),
             event,
             unquote(clause.pattern),
             unquote(clause.state)
           )
           when event in unquote(clause.events) do
        unquote(clause.body)
      end
    end
  end

  defp dispatch_clause(clause) do
    message_pattern =
      case clause.params_pattern do
        nil ->
          quote do: %VerifiedPubSub.Message{payload: unquote(clause.pattern)}

        params ->
          quote do: %VerifiedPubSub.Message{
                  payload: unquote(clause.pattern),
                  params: unquote(params)
                }
      end

    quote line: clause.line do
      defp __verified_pubsub_dispatch__(
             unquote(clause.topic),
             event,
             unquote(message_pattern),
             unquote(clause.state)
           )
           when event in unquote(clause.events) do
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

  # The generated handle_info/2 always calls __verified_pubsub_dispatch__/4, so the
  # function must exist even when the module has no handle_message or ignore_message
  # call. A raise is correct here: a verified message arrived, but this module declared
  # no interest in it.
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

  defp put_clause!(caller, topic, params, event, pattern, state, body) do
    topic = literal_atom!(topic, :topic)
    whole_message? = message_pattern?(pattern, caller)

    if params && whole_message? do
      raise CompileError,
        file: caller.file,
        line: caller.line,
        description: """
        this clause already matches the params, so the payload argument cannot also be a \
        whole %VerifiedPubSub.Message{} pattern — the two would match the same message \
        twice, and the params match would be silently ignored.

        Match the payload instead.
        """
    end

    if params, do: validate_params_pattern!(caller, topic, params)
    events = literal_events!(:handle_message, topic, event)

    if !whole_message? do
      Enum.each(events, &validate_payload_pattern!(caller, topic, &1, pattern))
    end

    Module.put_attribute(caller.module, :verified_pubsub_clauses, %{
      topic: topic,
      events: events,
      pattern: pattern,
      params_pattern: params,
      state: state,
      body: body,
      whole_message?: whole_message?,
      line: caller.line
    })

    nil
  end

  defp literal_atom!(ast, _role) when is_atom(ast), do: ast

  defp literal_atom!(ast, role) do
    raise ArgumentError,
          "expected a literal atom for #{role}, got: #{Macro.to_string(ast)}. " <>
            "Topic and event must be literals so coverage can be checked at compile time."
  end

  # A struct pattern is clear in the AST. The check also accepts `%Message{...} = var`,
  # because it looks at both sides of a top-level match.
  defp message_pattern?({:=, _, [left, right]}, env) do
    message_pattern?(left, env) or message_pattern?(right, env)
  end

  defp message_pattern?({:%, _, [alias_ast, {:%{}, _, _}]}, env) do
    Macro.expand(alias_ast, env) == VerifiedPubSub.Message
  end

  defp message_pattern?(_, _), do: false

  # Only an unexpected key is an error. A match on a subset of the params is the normal
  # case, so a missing key is not a problem here, unlike in `broadcast!/4`.
  defp validate_params_pattern!(caller, topic, {:%{}, _, pairs}) when is_list(pairs) do
    manifest = Module.get_attribute(caller.module, :verified_pubsub_manifest)

    with keys when is_list(keys) <- literal_keys(pairs) do
      declared = VerifiedPubSub.Info.params(manifest, topic)

      case keys -- declared do
        [] ->
          :ok

        unexpected ->
          topic_struct = VerifiedPubSub.Info.topic!(manifest, topic)
          declared_at = VerifiedPubSub.Topic.declared_at(inspect(topic), topic_struct)

          raise CompileError,
            file: caller.file,
            line: caller.line,
            description: """
            topic #{inspect(topic)} has no #{Enum.map_join(unexpected, ", ", &inspect/1)}.

            #{inspect(topic)} takes #{inspect(declared)}, so this pattern could never match.#{declared_at}
            """
      end
    end

    :ok
  end

  defp validate_params_pattern!(_caller, _topic, _params), do: :ok

  # Returns the keys of a literal map, or nil when the AST is not a map that we can read
  # at compile time. `%{base | k: v}` arrives as one {:|, meta, [_, _]} tuple, not as
  # pairs.
  defp literal_keys(pairs) do
    if Enum.all?(pairs, &match?({key, _value} when is_atom(key), &1)) do
      Enum.map(pairs, &elem(&1, 0))
    end
  end

  defp literal_events!(macro, topic, []) do
    raise ArgumentError,
          "#{macro} #{inspect(topic)}, [] names no event. " <>
            "Pass at least one event, or remove the call."
  end

  defp literal_events!(_macro, _topic, events) when is_list(events) do
    Enum.map(events, &literal_atom!(&1, :event))
  end

  defp literal_events!(_macro, _topic, event), do: [literal_atom!(event, :event)]

  # A broadcast checks that a payload holds only declared fields. A literal pattern that
  # names any other key thus can never match. An error is better than a clause that
  # never runs and gives no sign. The most likely cause is a params map in the place of
  # the payload, which is the argument order of `broadcast!` without the event.
  defp validate_payload_pattern!(caller, topic, event, {:%{}, _, pairs}) when is_list(pairs) do
    manifest = Module.get_attribute(caller.module, :verified_pubsub_manifest)

    with keys when is_list(keys) <- literal_keys(pairs) do
      declared =
        manifest
        |> VerifiedPubSub.Info.fields(topic, event)
        |> Enum.map(& &1.name)

      case keys -- declared do
        [] ->
          :ok

        undeclared ->
          raise CompileError,
            file: caller.file,
            line: caller.line,
            description: payload_pattern_message(manifest, topic, event, declared, undeclared)
      end
    end

    :ok
  end

  defp validate_payload_pattern!(_caller, _topic, _event, _pattern), do: :ok

  defp payload_pattern_message(manifest, topic, event, declared, undeclared) do
    topic_struct = VerifiedPubSub.Info.topic!(manifest, topic)
    params = topic_struct.params
    message = Enum.find(topic_struct.messages, &(&1.name == event))

    declared_at =
      VerifiedPubSub.Topic.declared_at("#{inspect(event)} on #{inspect(topic)}", message)

    hint =
      if undeclared != [] and Enum.all?(undeclared, &(&1 in params)) do
        """

        #{inspect(undeclared)} #{if length(undeclared) == 1, do: "is a param", else: "are params"} \
        of #{inspect(topic)}, not a payload field. To match on params, put them where \
        `broadcast!/4` takes them — right after the topic:

            handle_message #{inspect(topic)}, %{#{Enum.map_join(params, ", ", &"#{&1}: ...")}}, \
        #{inspect(event)}, payload, state do
        """
      else
        ""
      end

    """
    #{inspect(event)} on #{inspect(topic)} declares no #{Enum.map_join(undeclared, ", ", &inspect/1)}.

    A payload carries exactly the declared fields #{inspect(declared)}, so this pattern \
    could never match.#{declared_at}
    #{hint}\
    """
  end
end
