defmodule VerifiedPubSub.Topic do
  @moduledoc """
  Builds the segment of a wire topic that a param fills.

  A topic is declared as a list of segments joined with `:`, where a string is a literal
  and an atom is a param. Three properties together guarantee that an interpolated topic
  can only ever be the topic its call site names:

    1. every param fills a whole segment — guaranteed by the list shape, since a param is
       its own element and literals may not contain the separator
    2. no two declared topics can match the same topic string — checked when the manifest
       compiles
    3. a param value is non-empty and free of the separator — checked here, per call

  Dropping any one of them reopens the hole. `["a", :x]` and `["a", :x, "b"]` differ in
  segment count, so (2) holds, yet `x = "1:b"` on the first builds `"a:1:b"` — precisely
  what the second builds from `x = "1"`. Without (3) a broadcast reaches the other topic's
  subscribers.
  """

  @separator ":"

  @doc "The character that separates topic segments."
  @spec separator() :: String.t()
  def separator, do: @separator

  @doc """
  Stringifies `value` and checks it can fill one segment of the topic.

  Raises `VerifiedPubSub.TopicError` if it cannot.
  """
  @spec segment!(module(), atom(), atom(), term()) :: String.t()
  def segment!(manifest, topic, param, value) do
    string = to_string(value)

    cond do
      string == "" ->
        raise_error(manifest, topic, param, value, :empty)

      String.contains?(string, @separator) ->
        raise_error(manifest, topic, param, value, :separator)

      true ->
        string
    end
  end

  defp raise_error(manifest, topic, param, value, reason) do
    raise VerifiedPubSub.TopicError,
      manifest: manifest,
      topic: topic,
      segments: VerifiedPubSub.Info.topic!(manifest, topic).segments,
      param: param,
      value: value,
      reason: reason
  end
end
