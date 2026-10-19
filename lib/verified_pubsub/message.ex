defmodule VerifiedPubSub.Message do
  @moduledoc """
  The struct that a subscriber receives for each verified broadcast.

  `params` holds the param values that built the wire topic. A process can subscribe to
  one topic with many sets of params, and `params` tells the messages apart.
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
