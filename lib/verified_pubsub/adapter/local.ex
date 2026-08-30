defmodule VerifiedPubsub.Adapter.Local do
  @moduledoc """
  In-VM adapter that delivers with `send/2`. Intended for tests and for
  single-node use; it does not cross nodes.

  Must be started before use:

      children = [VerifiedPubsub.Adapter.Local]

  The `config` passed by the registry (the `:pubsub` option) is the name of the
  `Registry` process to use. It defaults to `#{inspect(__MODULE__)}.Registry` when
  `nil`, so `use VerifiedPubsub.Registry, adapter: VerifiedPubsub.Adapter.Local`
  works with no further configuration. Pass a name to isolate independent instances:

      children = [{VerifiedPubsub.Adapter.Local, name: MyIsolatedRegistry}]
  """

  @behaviour VerifiedPubsub.Adapter

  @default_registry __MODULE__.Registry

  @doc "The registry name used when a registry supplies no `:pubsub` config."
  def default_registry, do: @default_registry

  def child_spec(opts) do
    opts
    |> Keyword.get(:name, @default_registry)
    |> then(&Registry.child_spec(keys: :duplicate, name: &1))
    |> Map.put(:id, Keyword.get(opts, :name, @default_registry))
  end

  @impl true
  def subscribe(config, topic) when is_binary(topic) do
    case Registry.register(registry(config), topic, nil) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def unsubscribe(config, topic) when is_binary(topic) do
    Registry.unregister(registry(config), topic)
  end

  @impl true
  def broadcast(config, topic, message) when is_binary(topic) do
    dispatch(config, topic, message, nil)
  end

  @impl true
  def broadcast_from(config, from, topic, message) when is_binary(topic) and is_pid(from) do
    dispatch(config, topic, message, from)
  end

  defp dispatch(config, topic, message, except) do
    Registry.dispatch(registry(config), topic, fn entries ->
      Enum.each(entries, fn
        {^except, _} -> :ok
        {pid, _} -> send(pid, message)
      end)
    end)
  end

  defp registry(nil), do: @default_registry
  defp registry(name) when is_atom(name), do: name
end
