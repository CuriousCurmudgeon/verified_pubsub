defmodule VerifiedPubsub.Broadcast do
  @moduledoc false

  # This exists so the generated `broadcast_*!` functions do not contain the
  # `{:error, reason}` clause themselves. An adapter whose `broadcast/3` is inferred to
  # return only `:ok` (as `Adapter.Local` is, since `Registry.dispatch/3` always
  # succeeds) would make that clause dead code, and Elixir would emit a
  # "clause will never match" warning inside every consumer's generated registry.
  # Here the argument keeps the behaviour's declared `:ok | {:error, term}` type.

  @doc false
  @spec bang!(:ok | {:error, term()}, atom(), atom()) :: :ok
  def bang!(:ok, _topic, _event), do: :ok

  def bang!({:error, reason}, topic, event) do
    raise "failed to broadcast #{inspect(event)} on #{inspect(topic)}: #{inspect(reason)}"
  end
end
