defmodule VerifiedPubSub.Subscriber.Verify do
  @moduledoc """
  Compile-time coverage check for `VerifiedPubSub.Subscriber`.

  Coverage is tracked per `{topic, event}` pair using **set** semantics, because
  several `handle_message` clauses for one event are legal and expected when matching
  on param values.

  Coverage is therefore name-based, not value-based: if every clause for an event
  matches a narrow param value, the event still counts as covered, and a message with
  any other value raises `FunctionClauseError` at runtime. No static check closes that
  gap — it is value coverage, not name coverage. End with a param-agnostic clause when
  matching on param values.
  """

  alias VerifiedPubSub.Info

  @doc false
  def run!(env, clauses, ignored) do
    registry = Module.get_attribute(env.module, :verified_pubsub_registry)
    topics = Module.get_attribute(env.module, :verified_pubsub_topics)
    on_missing = Module.get_attribute(env.module, :verified_pubsub_on_missing)

    declared =
      for topic <- topics, event <- Info.events(registry, topic), into: MapSet.new() do
        {topic, event}
      end

    accounted_for = MapSet.new(Enum.map(clauses, &{&1.topic, &1.event}) ++ ignored)

    undeclared = MapSet.difference(accounted_for, declared)
    missing = MapSet.difference(declared, accounted_for)

    unless Enum.empty?(undeclared) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description: undeclared_message(env, registry, topics, undeclared)
    end

    if not Enum.empty?(missing) and on_missing != :ignore do
      description = missing_message(env, registry, missing)

      case on_missing do
        :error -> raise CompileError, file: env.file, line: env.line, description: description
        :warn -> IO.warn(description, env)
      end
    end

    :ok
  end

  defp undeclared_message(env, registry, topics, undeclared) do
    detail =
      undeclared
      |> Enum.sort()
      |> Enum.map_join("\n", fn {topic, event} ->
        if topic in topics do
          "  * #{inspect(topic)}, #{inspect(event)} — #{inspect(registry)} declares " <>
            "#{inspect(Info.events(registry, topic))} on #{inspect(topic)}"
        else
          "  * #{inspect(topic)}, #{inspect(event)} — #{inspect(topic)} is not in the " <>
            ":topics list #{inspect(topics)}"
        end
      end)

    """
    #{inspect(env.module)} handles messages that #{inspect(registry)} does not declare:

    #{detail}

    Fix the topic or event name, add the topic to the :topics option of
    `use VerifiedPubSub.Subscriber`, or declare it in the registry.
    """
  end

  defp missing_message(env, registry, missing) do
    sorted = Enum.sort(missing)
    detail = Enum.map_join(sorted, "\n", fn {t, e} -> "  * #{inspect(t)}, #{inspect(e)}" end)
    {example_topic, example_event} = hd(sorted)

    """
    #{inspect(env.module)} subscribes to topics with events it does not account for:

    #{detail}

    Add a `handle_message` clause for each, or dismiss it explicitly:

        ignore_message #{inspect(example_topic)}, #{inspect(example_event)}

    Registry: #{inspect(registry)}
    """
  end
end
