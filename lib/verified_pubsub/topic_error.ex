defmodule VerifiedPubSub.TopicError do
  @moduledoc """
  Raised when a topic param value cannot safely fill its segment of the wire pattern.

  A param value fills exactly one segment of the pattern. A value that is empty, or that
  contains the segment separator, would build a topic string other than the one the call
  site names — potentially another declared topic, delivering the message to the wrong
  subscribers. See `VerifiedPubSub.Topic`.
  """

  defexception [:manifest, :topic, :pattern, :param, :value, :reason]

  @impl true
  def message(%__MODULE__{} = error) do
    """
    invalid value for topic param #{inspect(error.param)} on #{inspect(error.topic)} in \
    #{inspect(error.manifest)}:

      #{detail(error)}

    A param value fills exactly one segment of #{inspect(error.pattern)}, so it must be \
    non-empty and must not contain #{inspect(VerifiedPubSub.Topic.separator())}. Otherwise \
    it could build a topic other than the one this call names.
    """
  end

  defp detail(%{reason: :empty}), do: "the value is empty"

  defp detail(%{reason: :separator, value: value}) do
    "#{inspect(value)} contains #{inspect(VerifiedPubSub.Topic.separator())}"
  end
end
