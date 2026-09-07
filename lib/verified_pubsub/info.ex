defmodule VerifiedPubSub.Info do
  @moduledoc """
  The only supported way to read a manifest.

  Everything outside this module — including `VerifiedPubSub.Subscriber` — goes
  through these functions rather than Spark internals, so the DSL front-end stays
  replaceable.
  """

  use Spark.InfoGenerator, extension: VerifiedPubSub.Dsl, sections: [:topics]

  alias VerifiedPubSub.Dsl.Topic

  @doc "Fetches a topic by its alias."
  @spec topic(module(), atom()) :: {:ok, Topic.t()} | :error
  def topic(manifest, name) do
    case Enum.find(topics(manifest), &(&1.name == name)) do
      nil -> :error
      topic -> {:ok, topic}
    end
  end

  @doc "Fetches a topic by its alias, raising with the known topics if absent."
  @spec topic!(module(), atom()) :: Topic.t()
  def topic!(manifest, name) do
    case topic(manifest, name) do
      {:ok, topic} ->
        topic

      :error ->
        known = manifest |> topics() |> Enum.map(& &1.name) |> Enum.sort()

        raise ArgumentError,
              "unknown topic #{inspect(name)} in #{inspect(manifest)}. " <>
                "Known topics: #{inspect(known)}"
    end
  end

  @doc "Event names declared on a topic, in declaration order."
  @spec events(module(), atom()) :: [atom()]
  def events(manifest, name) do
    manifest |> topic!(name) |> Map.fetch!(:messages) |> Enum.map(& &1.name)
  end

  @doc "Declared payload fields for an event, in declaration order."
  @spec fields(module(), atom(), atom()) :: [VerifiedPubSub.Dsl.Field.t()]
  def fields(manifest, topic, event) do
    topic_struct = topic!(manifest, topic)

    case Enum.find(topic_struct.messages, &(&1.name == event)) do
      nil ->
        raise ArgumentError,
              "unknown event #{inspect(event)} on topic #{inspect(topic)} in " <>
                "#{inspect(manifest)}. Declared events: #{inspect(events(manifest, topic))}"

      message ->
        message.fields
    end
  end

  @doc "Parameter names for a topic, in the order they appear in the pattern."
  @spec params(module(), atom()) :: [atom()]
  def params(manifest, name) do
    manifest |> topic!(name) |> Map.fetch!(:params)
  end
end
