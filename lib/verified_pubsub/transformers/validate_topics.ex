defmodule VerifiedPubSub.Transformers.ValidateTopics do
  @moduledoc """
  Validates the registry, raising at compile time.

  This is a Transformer rather than a Verifier deliberately: a Verifier returning
  `{:error, _}` runs via `@after_verify`, which downgrades the error to a warning and
  still defines the module. A Transformer returning `{:error, _}` aborts compilation.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def after?(VerifiedPubSub.Transformers.ParseParams), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    topics = Transformer.get_entities(dsl, [:topics])
    module = Transformer.get_persisted(dsl, :module)

    with :ok <- validate_unique_topics(topics, module),
         :ok <- validate_topics(topics, module),
         :ok <- validate_whole_segments(topics, module),
         :ok <- validate_disjoint(topics, module) do
      {:ok, dsl}
    end
  end

  defp validate_unique_topics(topics, module) do
    names = Enum.map(topics, & &1.name)

    case names -- Enum.uniq(names) do
      [] -> :ok
      [dup | _] -> error(module, [:topics, dup], "duplicate topic #{inspect(dup)}")
    end
  end

  defp validate_topics(topics, module) do
    Enum.reduce_while(topics, :ok, fn topic, :ok ->
      case validate_topic(topic, module) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_topic(topic, module) do
    events = Enum.map(topic.messages, & &1.name)
    duplicates = events -- Enum.uniq(events)

    cond do
      events == [] ->
        error(
          module,
          [:topics, topic.name],
          "topic #{inspect(topic.name)} declares no events. Add at least one `message`, " <>
            "or remove the topic."
        )

      duplicates != [] ->
        error(
          module,
          [:topics, topic.name, hd(duplicates)],
          "duplicate event #{inspect(hd(duplicates))} on topic #{inspect(topic.name)}"
        )

      true ->
        :ok
    end
  end

  # A param must fill a whole colon-delimited segment. Otherwise a value can complete a
  # literal and forge a different topic: with "a:x%{p}", a value of "y" builds "a:xy",
  # which is exactly what a declared "a:xy" builds. `~p` restricts path interpolation the
  # same way, for the same reason.
  defp validate_whole_segments(topics, module) do
    Enum.reduce_while(topics, :ok, fn topic, :ok ->
      case offending_segment(topic.pattern) do
        nil ->
          {:cont, :ok}

        segment ->
          {:halt,
           error(
             module,
             [:topics, topic.name],
             """
             #{inspect(segment)} in pattern #{inspect(topic.pattern)} mixes a parameter \
             with other text.

             A parameter must be a whole #{inspect(VerifiedPubSub.Topic.separator())}\
             -delimited segment, so that its value cannot complete a literal and build a \
             different topic. Give it a segment of its own.
             """
           )}
      end
    end)
  end

  defp offending_segment(pattern) do
    pattern
    |> String.split(VerifiedPubSub.Topic.separator())
    |> Enum.find(fn segment ->
      String.contains?(segment, "%{") and not Regex.match?(~r/^%\{[^}]*\}$/, segment)
    end)
  end

  # Two patterns that can match the same topic string make delivery ambiguous: a subscriber
  # of one receives the other's messages. A router tolerates overlapping routes because it
  # only answers "did something match" and breaks ties by declaration order; a topic has to
  # resolve to one identity, because that is what the event and payload are checked against.
  defp validate_disjoint(topics, module) do
    pairs =
      for a <- topics,
          b <- topics,
          a.name < b.name,
          overlap?(segment_shape(a.pattern), segment_shape(b.pattern)),
          do: {a, b}

    case pairs do
      [] ->
        :ok

      [{a, b} | _] ->
        error(
          module,
          [:topics, a.name],
          """
          topics #{inspect(a.name)} and #{inspect(b.name)} can match the same topic string:

            #{inspect(a.name)} — #{inspect(a.pattern)}
            #{inspect(b.name)} — #{inspect(b.pattern)}

          A subscriber of one would receive the other's messages. Distinguish them with a \
          literal segment.
          """
        )
    end
  end

  # Each colon-delimited segment is either a literal or :param, which stands for any single
  # segment. Safe because a param is already known to fill a whole segment.
  defp segment_shape(pattern) do
    pattern
    |> String.split(VerifiedPubSub.Topic.separator())
    |> Enum.map(&if(String.contains?(&1, "%{"), do: :param, else: &1))
  end

  defp overlap?(a, b) when length(a) == length(b) do
    Enum.zip(a, b)
    |> Enum.all?(fn
      {:param, _} -> true
      {_, :param} -> true
      {x, y} -> x == y
    end)
  end

  defp overlap?(_a, _b), do: false

  defp error(module, path, message) do
    {:error, Spark.Error.DslError.exception(message: message, path: path, module: module)}
  end
end
