defmodule VerifiedPubSub.Subscriber.Verify do
  @moduledoc """
  Checks at compile time that a `VerifiedPubSub.Subscriber` covers each event.

  The `handle_message/5` and `ignore_message/2` calls of the module set the topics that
  it subscribes to. The check then tracks coverage for each `{topic, event}` pair as a
  **set**. An event can legally have many `handle_message` clauses, when they match
  different param values.

  ## Coverage of param values

  Coverage thus depends on event names, not on param values. When each clause for an
  event matches only some param values, the event still counts as covered. A message
  with any other value then raises `FunctionClauseError` at runtime. No static check can
  close that gap, because it is a gap in value coverage, not in name coverage. When the
  clauses of an event match on param values, end with a clause that matches any value.
  """

  alias VerifiedPubSub.Info

  @doc false
  def run!(env, clauses, ignored) do
    manifest = Module.get_attribute(env.module, :verified_pubsub_manifest)

    handled = MapSet.new(Enum.map(clauses, &{&1.topic, &1.event}))
    dismissed = MapSet.new(ignored)
    accounted_for = MapSet.union(handled, dismissed)

    contradictory = MapSet.intersection(handled, dismissed)

    if MapSet.size(contradictory) > 0 do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: contradictory_message(env, contradictory)
    end

    # The subscribed topics are the topics that the module names. This read of the
    # manifest also creates the compile-time dependency on it, so a change to the
    # manifest recompiles every subscriber.
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

    if MapSet.size(undeclared) > 0 do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: undeclared_message(env, manifest, undeclared)
    end

    if MapSet.size(missing) > 0 do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: missing_message(env, manifest, missing)
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
        raise CompileError,
          file: env.file,
          line: env.line,
          description: """
          #{inspect(env.module)} handles messages on unknown topic #{inspect(topic)}.

          #{inspect(manifest)} declares:
          #{VerifiedPubSub.Topic.listing(Info.topics(manifest))}
          """
    end
  end

  defp undeclared_message(env, manifest, undeclared) do
    detail =
      undeclared
      |> Enum.sort()
      |> Enum.map_join("\n", fn {topic, event} ->
        topic_struct = Info.topic!(manifest, topic)

        "  * #{inspect(topic)}, #{inspect(event)} — #{inspect(manifest)} declares " <>
          "#{inspect(Info.events(manifest, topic))} on " <>
          VerifiedPubSub.Topic.describe(topic_struct) <> at(topic_struct)
      end)

    """
    #{inspect(env.module)} handles messages that #{inspect(manifest)} does not declare:

    #{detail}

    Fix the event name, or declare it in the manifest.
    """
  end

  defp at(entity) do
    case VerifiedPubSub.Topic.location(entity) do
      nil -> ""
      location -> " at #{location}"
    end
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
