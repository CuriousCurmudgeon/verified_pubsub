defmodule VerifiedPubSub.Transformers.ValidateTopics do
  @moduledoc """
  Validates the topics and events of the manifest at compile time.

  This module is a Transformer, not a Verifier, on purpose. A Verifier that returns
  `{:error, _}` runs from `@after_verify`, which changes the error to a warning and still
  defines the module. A Transformer that returns `{:error, _}` stops the compilation.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer
  alias VerifiedPubSub.Topic

  @impl true
  def after?(VerifiedPubSub.Transformers.ParseParams), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    topics = Transformer.get_entities(dsl, [:topics])
    module = Transformer.get_persisted(dsl, :module)

    with :ok <- validate_unique_topics(topics, module),
         :ok <- validate_topics(topics, module),
         :ok <- validate_disjoint(topics, module) do
      {:ok, dsl}
    end
  end

  defp validate_unique_topics(topics, module) do
    names = Enum.map(topics, & &1.name)

    case names -- Enum.uniq(names) do
      [] ->
        :ok

      [dup | _] ->
        [first, second | _] = Enum.filter(topics, &(&1.name == dup))

        error(
          module,
          [:topics, dup],
          "duplicate topic #{inspect(dup)}#{Topic.declared_at("The first", first)}",
          second
        )
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
            "or remove the topic.",
          topic
        )

      duplicates != [] ->
        dup = hd(duplicates)
        [first, second | _] = Enum.filter(topic.messages, &(&1.name == dup))

        error(
          module,
          [:topics, topic.name, dup],
          "duplicate event #{inspect(dup)} on topic #{inspect(topic.name)}" <>
            Topic.declared_at("The first", first),
          second
        )

      true ->
        :ok
    end
  end

  # When two topics can match the same wire topic, a subscriber of one receives the
  # messages of the other. A router allows routes that overlap, because it only checks for
  # a match and picks the first route that matches. But a wire topic must match exactly one
  # topic, because the library checks the event and the payload against that topic.
  defp validate_disjoint(topics, module) do
    pairs =
      for a <- topics,
          b <- topics,
          a.name < b.name,
          overlap?(segment_shape(a.segments), segment_shape(b.segments)),
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

            #{inspect(a.name)} — #{inspect(a.segments)}#{at(a)}
            #{inspect(b.name)} — #{inspect(b.segments)}#{at(b)}

          A subscriber of one would receive the other's messages. Distinguish them with a \
          literal segment.
          """,
          a
        )
    end
  end

  # This function replaces each param with :param, which matches any one segment. A
  # literal stays the same.
  defp segment_shape(segments) do
    Enum.map(segments, &if(is_atom(&1), do: :param, else: &1))
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

  defp at(topic) do
    case Topic.location(topic) do
      nil -> ""
      location -> ", at #{location}"
    end
  end

  # With the location of the entity, Spark points the error at that declaration. Spark
  # also reports the error as a diagnostic that an editor can show at the declaration.
  defp error(module, path, message, entity) do
    {:error,
     Spark.Error.DslError.exception(
       message: message,
       path: path,
       module: module,
       location: Topic.anno(entity)
     )}
  end
end
