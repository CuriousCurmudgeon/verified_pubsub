defmodule VerifiedPubsub.Broadcast do
  @moduledoc false

  # This exists so the generated `broadcast_*!` functions do not contain the
  # `{:error, reason}` clause themselves. If the broadcast call is ever inferred to
  # return only `:ok`, that clause becomes dead code and Elixir emits a "clause will
  # never match" warning inside every consumer's generated registry. Keeping the case
  # here, where the argument carries the declared `:ok | {:error, term}` type, means
  # generated code cannot trip that warning.
  #
  # It also gives a better message than Phoenix.PubSub.broadcast!/4 would, by naming
  # the topic and event rather than just the underlying failure.

  @doc false
  @spec bang!(:ok | {:error, term()}, atom(), atom()) :: :ok
  def bang!(:ok, _topic, _event), do: :ok

  def bang!({:error, reason}, topic, event) do
    raise "failed to broadcast #{inspect(event)} on #{inspect(topic)}: #{inspect(reason)}"
  end
end
