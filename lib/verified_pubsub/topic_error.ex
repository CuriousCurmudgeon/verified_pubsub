defmodule VerifiedPubSub.TopicError do
  @moduledoc """
  The error for a param value that cannot safely fill its segment of the wire topic.

  A param value fills exactly one segment of the topic. A value that is empty, or that
  contains the separator, can build a wire topic other than the one that the call site
  names. That wire topic can belong to another declared topic. The message then goes to
  the wrong subscribers. See `VerifiedPubSub.Topic`.
  """

  defexception [:manifest, :topic, :segments, :param, :value, :reason]

  @impl true
  def message(%__MODULE__{} = error) do
    """
    invalid value for topic param #{inspect(error.param)} on #{inspect(error.topic)} \
    (#{VerifiedPubSub.Topic.pattern(error.segments)}) in #{inspect(error.manifest)}:

      #{detail(error)}

    A param value fills exactly one segment, its <#{error.param}>, so it must be \
    non-empty and must not contain #{inspect(VerifiedPubSub.Topic.separator())}. Otherwise \
    it could build a topic other than the one this call names.
    """
  end

  defp detail(%{reason: :empty}), do: "the value is empty"

  defp detail(%{reason: :separator, value: value}) do
    "#{inspect(value)} contains #{inspect(VerifiedPubSub.Topic.separator())}"
  end
end
