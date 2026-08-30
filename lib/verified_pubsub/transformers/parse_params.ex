defmodule VerifiedPubSub.Transformers.ParseParams do
  @moduledoc """
  Derives each topic's parameter list from its wire pattern, so params are declared
  exactly once.
  """

  use Spark.Dsl.Transformer

  @param_regex ~r/%\{([^}]*)\}/
  @identifier_regex ~r/^[a-z_][a-zA-Z0-9_]*$/

  @doc """
  Extracts parameter names from a wire pattern.

      iex> VerifiedPubSub.Transformers.ParseParams.parse("accounts:%{account_id}:campaigns")
      {:ok, [:account_id]}
  """
  @spec parse(String.t()) :: {:ok, [atom()]} | {:error, String.t()}
  def parse(pattern) when is_binary(pattern) do
    found = Regex.scan(@param_regex, pattern, capture: :all_but_first) |> List.flatten()
    opens = pattern |> String.split("%{") |> length() |> Kernel.-(1)

    cond do
      opens != length(found) ->
        {:error, "unterminated parameter in pattern #{inspect(pattern)}: expected a closing `}`"}

      Enum.any?(found, &(&1 == "")) ->
        {:error, "empty parameter name in pattern #{inspect(pattern)}"}

      invalid = Enum.find(found, &(not Regex.match?(@identifier_regex, &1))) ->
        {:error,
         "invalid parameter name #{inspect(invalid)} in pattern #{inspect(pattern)}: " <>
           "must be a lowercase Elixir identifier"}

      true ->
        params = Enum.map(found, &String.to_atom/1)
        duplicates = params -- Enum.uniq(params)

        if duplicates == [] do
          {:ok, params}
        else
          {:error,
           "duplicate parameter #{inspect(hd(duplicates))} in pattern #{inspect(pattern)}"}
        end
    end
  end

  @impl true
  def transform(dsl) do
    dsl
    |> Spark.Dsl.Transformer.get_entities([:topics])
    |> Enum.reduce_while({:ok, dsl}, fn topic, {:ok, acc} ->
      case parse(topic.pattern) do
        {:ok, params} ->
          {:cont, {:ok, replace_topic(acc, %{topic | params: params})}}

        {:error, message} ->
          {:halt,
           {:error,
            Spark.Error.DslError.exception(
              message: message,
              path: [:topics, topic.name],
              module: Spark.Dsl.Transformer.get_persisted(dsl, :module)
            )}}
      end
    end)
  end

  defp replace_topic(dsl, topic) do
    Spark.Dsl.Transformer.replace_entity(dsl, [:topics], topic, &(&1.name == topic.name))
  end
end
