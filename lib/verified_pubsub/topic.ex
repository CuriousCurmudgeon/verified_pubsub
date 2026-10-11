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
  Renders segments as a wire pattern, with each param in angle brackets.

      iex> VerifiedPubSub.Topic.pattern(["accounts", :account_id, "campaigns"])
      "accounts:<account_id>:campaigns"

  This is the form errors show a topic in, so a reader holding a wire string from a log
  can tell which declared topic it belongs to.
  """
  @spec pattern([VerifiedPubSub.Dsl.Topic.segment()]) :: String.t()
  def pattern(segments) do
    Enum.map_join(segments, @separator, fn
      literal when is_binary(literal) -> literal
      param -> "<#{param}>"
    end)
  end

  @doc false
  # `:campaigns (accounts:<account_id>:campaigns)`, for naming one topic in a message.
  @spec describe(VerifiedPubSub.Dsl.Topic.t()) :: String.t()
  def describe(topic), do: "#{inspect(topic.name)} (#{pattern(topic.segments)})"

  @doc false
  # Every topic in a manifest as aligned `name  pattern` lines, sorted by name, for errors
  # that list the alternatives.
  @spec listing([VerifiedPubSub.Dsl.Topic.t()]) :: String.t()
  def listing(topics) do
    sorted = Enum.sort_by(topics, & &1.name)
    width = sorted |> Enum.map(&String.length(inspect(&1.name))) |> Enum.max(fn -> 0 end)

    Enum.map_join(sorted, "\n", fn topic ->
      "  " <> String.pad_trailing(inspect(topic.name), width) <> "   " <> pattern(topic.segments)
    end)
  end

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
