defmodule VerifiedPubsub.Adapter.Local do
  @moduledoc """
  In-VM adapter that delivers with `send/2`. Intended for tests and for
  single-node use; it does not cross nodes.

  Must be started before use, e.g. in a supervision tree or via
  `start_supervised!(VerifiedPubsub.Adapter.Local)` in tests.
  """

  @behaviour VerifiedPubsub.Adapter

  @registry __MODULE__.Registry

  def child_spec(_opts) do
    Registry.child_spec(keys: :duplicate, name: @registry)
  end

  @impl true
  def subscribe(_config, topic) when is_binary(topic) do
    case Registry.register(@registry, topic, nil) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def unsubscribe(_config, topic) when is_binary(topic) do
    Registry.unregister(@registry, topic)
  end

  @impl true
  def broadcast(_config, topic, message) when is_binary(topic) do
    Registry.dispatch(@registry, topic, fn entries ->
      Enum.each(entries, fn {pid, _} -> send(pid, message) end)
    end)
  end
end
