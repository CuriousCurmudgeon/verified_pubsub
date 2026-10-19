defmodule VerifiedPubSub.Broadcast do
  @moduledoc false

  # This module holds the `{:error, reason}` clause, so the generated `broadcast_*!`
  # functions do not hold it. When the compiler infers that the broadcast call returns
  # only `:ok`, that clause is dead code. Elixir then emits a "clause will never match"
  # warning in the code that each manifest generates. Here, the argument has the declared
  # `:ok | {:error, term}` type, so generated code cannot cause that warning.
  #
  # The error that it raises also names the topic and the event. The error of
  # Phoenix.PubSub.broadcast!/4 names only the failure underneath.

  @doc false
  @spec bang!(:ok | {:error, term()}, atom(), String.t(), atom()) :: :ok
  def bang!(:ok, _topic, _wire, _event), do: :ok

  def bang!({:error, reason}, topic, wire, event) do
    raise "failed to broadcast #{inspect(event)} on #{inspect(topic)} (#{inspect(wire)}): " <>
            inspect(reason)
  end
end
