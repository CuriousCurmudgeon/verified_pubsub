defmodule VerifiedPubSub.Message do
  @moduledoc """
  The struct delivered to subscribers for every verified broadcast.

  `params` carries the values interpolated into a parameterized topic, so a
  process subscribed to several instances of a topic can tell them apart.
  """

  @type t :: %__MODULE__{
          manifest: module(),
          topic: atom(),
          event: atom(),
          params: map(),
          payload: term()
        }

  @enforce_keys [:manifest, :topic, :event]
  defstruct [:manifest, :topic, :event, params: %{}, payload: nil]
end
