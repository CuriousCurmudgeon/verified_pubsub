defmodule VerifiedPubsub.Message do
  @moduledoc """
  The struct delivered to subscribers for every verified broadcast.

  `params` carries the values interpolated into a parameterized topic, so a
  process subscribed to several instances of a topic can tell them apart.
  """

  @type t :: %__MODULE__{
          registry: module(),
          topic: atom(),
          event: atom(),
          params: map(),
          payload: term()
        }

  @enforce_keys [:registry, :topic, :event]
  defstruct [:registry, :topic, :event, params: %{}, payload: nil]
end
