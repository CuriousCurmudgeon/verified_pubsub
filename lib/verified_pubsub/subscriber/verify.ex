defmodule VerifiedPubSub.Subscriber.Verify do
  @moduledoc """
  Compile-time coverage check for `VerifiedPubSub.Subscriber`.

  The subscribed topics are inferred from the module's `handle_message/5` and
  `ignore_message/2` calls. Coverage is then tracked per `{topic, event}` pair using
  **set** semantics, because several `handle_message` clauses for one event are legal and
  expected when matching on param values.

  Coverage is therefore name-based, not value-based: if every clause for an event
  matches a narrow param value, the event still counts as covered, and a message with
  any other value raises `FunctionClauseError` at runtime. No static check closes that
  gap — it is value coverage, not name coverage. End with a param-agnostic clause when
  matching on param values.
  """

  alias VerifiedPubSub.Info

  @doc false
  def run!(env, clauses, ignored) do
    manifest = Module.get_attribute(env.module, :verified_pubsub_manifest)
    on_missing = Module.get_attribute(env.module, :verified_pubsub_on_missing)

    handled = MapSet.new(Enum.map(clauses, &{&1.topic, &1.event}))
    dismissed = MapSet.new(ignored)
    accounted_for = MapSet.union(handled, dismissed)

    contradictory = MapSet.intersection(handled, dismissed)

    unless Enum.empty?(contradictory) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: contradictory_message(env, contradictory)
    end

    # The subscribed topics are whatever the module actually mentions. Reading the
    # manifest here is also what creates the compile-time dependency on it, so editing
    # the manifest recompiles every subscriber.
    topics =
      accounted_for
      |> Enum.map(&elem(&1, 0))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&validate_topic!(env, manifest, &1))

    declared =
      for topic <- topics, event <- Info.events(manifest, topic), into: MapSet.new() do
        {topic, event}
      end

    undeclared = MapSet.difference(accounted_for, declared)
    missing = MapSet.difference(declared, accounted_for)

    unless Enum.empty?(undeclared) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: undeclared_message(env, manifest, undeclared)
    end

    if not Enum.empty?(missing) and on_missing != :ignore do
      description = missing_message(env, manifest, missing)

      case on_missing do
        :error -> raise CompileError, file: env.file, line: env.line, description: description
        :warn -> IO.warn(description, env)
      end
    end

    :ok
  end

  defp contradictory_message(env, contradictory) do
    detail =
      contradictory
      |> Enum.sort()
      |> Enum.map_join("\n", fn {topic, event} ->
        "  * #{inspect(topic)}, #{inspect(event)}"
      end)

    """
    #{inspect(env.module)} both handles and ignores the same event:

    #{detail}

    An event can be handled by `handle_message` or dismissed by `ignore_message`, not
    both. Remove whichever one you did not intend.
    """
  end

  defp validate_topic!(env, manifest, topic) do
    case Info.topic(manifest, topic) do
      {:ok, _} ->
        topic

      :error ->
        known = manifest |> Info.topics() |> Enum.map(& &1.name) |> Enum.sort()

        raise CompileError,
          file: env.file,
          line: env.line,
          description: """
          #{inspect(env.module)} handles messages on unknown topic #{inspect(topic)}.

          #{inspect(manifest)} declares: #{inspect(known)}
          """
    end
  end

  defp undeclared_message(env, manifest, undeclared) do
    detail =
      undeclared
      |> Enum.sort()
      |> Enum.map_join("\n", fn {topic, event} ->
        "  * #{inspect(topic)}, #{inspect(event)} — #{inspect(manifest)} declares " <>
          "#{inspect(Info.events(manifest, topic))} on #{inspect(topic)}"
      end)

    """
    #{inspect(env.module)} handles messages that #{inspect(manifest)} does not declare:

    #{detail}

    Fix the event name, or declare it in the manifest.
    """
  end

  defp missing_message(env, manifest, missing) do
    sorted = Enum.sort(missing)
    detail = Enum.map_join(sorted, "\n", fn {t, e} -> "  * #{inspect(t)}, #{inspect(e)}" end)
    {example_topic, example_event} = hd(sorted)

    """
    #{inspect(env.module)} subscribes to topics with events it does not account for:

    #{detail}

    Add a `handle_message` clause for each, or dismiss it explicitly:

        ignore_message #{inspect(example_topic)}, #{inspect(example_event)}

    Manifest: #{inspect(manifest)}
    """
  end
end
