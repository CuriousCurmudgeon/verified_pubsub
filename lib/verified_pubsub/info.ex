defmodule VerifiedPubSub.Info do
  @moduledoc """
  The only supported way to read a manifest.

  Read a manifest through these functions, not through the Spark internals, so that the
  DSL stays replaceable.
  """

  use Spark.InfoGenerator, extension: VerifiedPubSub.Dsl, sections: [:topics]

  alias VerifiedPubSub.Dsl.Topic

  @doc "Fetches a topic by its name."
  @spec topic(module(), atom()) :: {:ok, Topic.t()} | :error
  def topic(manifest, name) do
    case Enum.find(topics(manifest), &(&1.name == name)) do
      nil -> :error
      topic -> {:ok, topic}
    end
  end

  @doc "Fetches a topic by its name, and raises with the known topics when it is absent."
  @spec topic!(module(), atom()) :: Topic.t()
  def topic!(manifest, name) do
    case topic(manifest, name) do
      {:ok, topic} ->
        topic

      :error ->
        raise ArgumentError, """
        unknown topic #{inspect(name)} in #{inspect(manifest)}. Known topics:
        #{VerifiedPubSub.Topic.listing(topics(manifest))}
        """
    end
  end

  @doc "Returns the events that a topic declares, in the order of declaration."
  @spec events(module(), atom()) :: [atom()]
  def events(manifest, name) do
    manifest
    |> topic!(name)
    |> Map.fetch!(:messages)
    |> Enum.map(& &1.name)
  end

  @doc "Returns the fields that an event declares, in the order of declaration."
  @spec fields(module(), atom(), atom()) :: [VerifiedPubSub.Dsl.Field.t()]
  def fields(manifest, topic, event) do
    topic_struct = topic!(manifest, topic)

    case Enum.find(topic_struct.messages, &(&1.name == event)) do
      nil ->
        raise ArgumentError,
              "unknown event #{inspect(event)} on topic " <>
                "#{VerifiedPubSub.Topic.describe(topic_struct)} in " <>
                "#{inspect(manifest)}. Declared events: #{inspect(events(manifest, topic))}"

      message ->
        message.fields
    end
  end

  @doc """
  Finds the declared topic that a wire topic belongs to, and the params that it carries.

      iex> VerifiedPubSub.Info.match(MyApp.Topics, "owners:42:pets")
      {:ok, :pets, %{owner_id: "42"}}

  It does the reverse of `VerifiedPubSub.Api.topic/2`, for a string from a log, a trace,
  or `Phoenix.PubSub`. Param values come back as the strings that were on the wire.
  Returns `:error` when no topic matches, and when a param segment is empty, which no
  broadcast can produce.
  """
  @spec match(module(), String.t()) :: {:ok, atom(), %{atom() => String.t()}} | :error
  def match(manifest, wire) when is_binary(wire) do
    parts = String.split(wire, VerifiedPubSub.Topic.separator())

    Enum.find_value(topics(manifest), :error, fn topic ->
      with {:ok, params} <- match_segments(topic.segments, parts, %{}) do
        {:ok, topic.name, params}
      end
    end)
  end

  defp match_segments([], [], params), do: {:ok, params}

  defp match_segments([literal | segs], [literal | parts], params) when is_binary(literal),
    do: match_segments(segs, parts, params)

  defp match_segments([param | segs], [value | parts], params)
       when is_atom(param) and value != "",
       do: match_segments(segs, parts, Map.put(params, param, value))

  defp match_segments(_segments, _parts, _params), do: nil

  @doc "Returns the params of a topic, in the order of its segments."
  @spec params(module(), atom()) :: [atom()]
  def params(manifest, name) do
    manifest
    |> topic!(name)
    |> Map.fetch!(:params)
  end
end
