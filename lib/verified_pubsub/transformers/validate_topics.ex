defmodule VerifiedPubsub.Transformers.ValidateTopics do
  @moduledoc """
  Validates the registry, raising at compile time.

  This is a Transformer rather than a Verifier deliberately: a Verifier returning
  `{:error, _}` runs via `@after_verify`, which downgrades the error to a warning and
  still defines the module. A Transformer returning `{:error, _}` aborts compilation.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def after?(VerifiedPubsub.Transformers.ParseParams), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    topics = Transformer.get_entities(dsl, [:topics])
    module = Transformer.get_persisted(dsl, :module)

    with :ok <- validate_unique_topics(topics, module),
         :ok <- validate_topics(topics, module) do
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

  defp error(module, path, message) do
    {:error, Spark.Error.DslError.exception(message: message, path: path, module: module)}
  end
end
