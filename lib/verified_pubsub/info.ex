defmodule VerifiedPubSub.Info do
  @moduledoc """
  The only supported way to read a registry.

  Everything outside this module — including `VerifiedPubSub.Subscriber` — goes
  through these functions rather than Spark internals, so the DSL front-end stays
  replaceable.
  """

  use Spark.InfoGenerator, extension: VerifiedPubSub.Dsl, sections: [:topics]

  alias VerifiedPubSub.Dsl.Topic

  @doc "Fetches a topic by its alias."
  @spec topic(module(), atom()) :: {:ok, Topic.t()} | :error
  def topic(registry, name) do
    case Enum.find(topics(registry), &(&1.name == name)) do
      nil -> :error
      topic -> {:ok, topic}
    end
  end

  @doc "Fetches a topic by its alias, raising with the known topics if absent."
  @spec topic!(module(), atom()) :: Topic.t()
  def topic!(registry, name) do
    case topic(registry, name) do
      {:ok, topic} ->
        topic

      :error ->
        known = registry |> topics() |> Enum.map(& &1.name) |> Enum.sort()

        raise ArgumentError,
              "unknown topic #{inspect(name)} in #{inspect(registry)}. " <>
                "Known topics: #{inspect(known)}"
    end
  end

  @doc "Event names declared on a topic, in declaration order."
  @spec events(module(), atom()) :: [atom()]
  def events(registry, name) do
    registry |> topic!(name) |> Map.fetch!(:messages) |> Enum.map(& &1.name)
  end

  @doc "Parameter names for a topic, in the order they appear in the pattern."
  @spec params(module(), atom()) :: [atom()]
  def params(registry, name) do
    registry |> topic!(name) |> Map.fetch!(:params)
  end
end
