defmodule VerifiedPubSub.Transformers.ParseParams do
  @moduledoc """
  Validates each topic's segments and derives its parameter list from them, so params are
  declared exactly once.
  """

  use Spark.Dsl.Transformer

  @doc """
  Validates a segment list and extracts its parameter names, in order.

      iex> VerifiedPubSub.Transformers.ParseParams.parse(["accounts", :account_id, "campaigns"])
      {:ok, [:account_id]}
  """
  @spec parse([String.t() | atom()]) :: {:ok, [atom()]} | {:error, String.t()}
  def parse([]), do: {:error, "a topic needs at least one segment"}

  def parse(segments) when is_list(segments) do
    separator = VerifiedPubSub.Topic.separator()

    cond do
      Enum.any?(segments, &(&1 == "")) ->
        {:error, "empty literal segment in #{inspect(segments)}"}

      literal = Enum.find(segments, &(is_binary(&1) and String.contains?(&1, separator))) ->
        {:error,
         "literal segment #{inspect(literal)} in #{inspect(segments)} contains " <>
           "#{inspect(separator)}: give each part its own list element"}

      # Enum.find/2 would return nil for a nil segment, so filter instead.
      (invalid = Enum.filter(segments, &(&1 in [nil, true, false]))) != [] ->
        {:error,
         "#{inspect(hd(invalid))} in #{inspect(segments)} is not a parameter name: " <>
           "a segment must be a string literal or a parameter atom"}

      true ->
        params = Enum.filter(segments, &is_atom/1)
        duplicates = params -- Enum.uniq(params)

        if duplicates == [] do
          {:ok, params}
        else
          {:error, "duplicate parameter #{inspect(hd(duplicates))} in #{inspect(segments)}"}
        end
    end
  end

  @impl true
  def transform(dsl) do
    dsl
    |> Spark.Dsl.Transformer.get_entities([:topics])
    |> Enum.reduce_while({:ok, dsl}, fn topic, {:ok, acc} ->
      case parse(topic.segments) do
        {:ok, params} ->
          {:cont, {:ok, replace_topic(acc, topic, %{topic | params: params})}}

        {:error, message} ->
          {:halt,
           {:error,
            Spark.Error.DslError.exception(
              message: message,
              path: [:topics, topic.name],
              module: Spark.Dsl.Transformer.get_persisted(dsl, :module),
              location: VerifiedPubSub.Topic.anno(topic)
            )}}
      end
    end)
  end

  # Matched on the whole struct, not the name: a duplicate name is reported by
  # ValidateTopics, and by then each duplicate must still be its own declaration.
  defp replace_topic(dsl, original, topic) do
    Spark.Dsl.Transformer.replace_entity(dsl, [:topics], topic, &(&1 == original))
  end
end
