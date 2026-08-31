defmodule VerifiedPubSub.Topic do
  @moduledoc """
  Builds the segment of a wire topic that a param fills.

  Topics are colon-delimited, and the registry requires every `%{param}` to occupy a whole
  segment. Three properties together guarantee that an interpolated topic can only ever be
  the topic its call site names:

    1. every `%{param}` fills a whole segment — checked when the registry compiles
    2. no two declared patterns can match the same topic string — likewise
    3. a param value is non-empty and free of the separator — checked here, per call

  Dropping any one of them reopens the hole. `"a:%{x}"` and `"a:%{x}:b"` differ in segment
  count, so (2) holds, yet `x = "1:b"` on the first builds `"a:1:b"` — precisely what the
  second builds from `x = "1"`. Without (3) a broadcast reaches the other topic's
  subscribers.
  """

  @separator ":"

  @doc "The character that separates topic segments."
  @spec separator() :: String.t()
  def separator, do: @separator

  @doc """
  Stringifies `value` and checks it can fill one segment of the topic pattern.

  Raises `VerifiedPubSub.TopicError` if it cannot.
  """
  @spec segment!(module(), atom(), atom(), term()) :: String.t()
  def segment!(registry, topic, param, value) do
    string = to_string(value)

    cond do
      string == "" ->
        raise_error(registry, topic, param, value, :empty)

      String.contains?(string, @separator) ->
        raise_error(registry, topic, param, value, :separator)

      true ->
        string
    end
  end

  defp raise_error(registry, topic, param, value, reason) do
    raise VerifiedPubSub.TopicError,
      registry: registry,
      topic: topic,
      pattern: VerifiedPubSub.Info.topic!(registry, topic).pattern,
      param: param,
      value: value,
      reason: reason
  end
end
