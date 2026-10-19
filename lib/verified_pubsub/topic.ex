defmodule VerifiedPubSub.Topic do
  @moduledoc """
  Builds the segment of a wire topic that a param fills.

  A manifest declares a topic as a list of segments, and `:` joins the segments. A string
  is a literal, and an atom is a param.

  ## Why each call checks the param value

  When three properties hold, an interpolated topic is always the topic that its call site
  names:

    1. Each param fills a whole segment. The list shape gives this property: a param is
       its own element, and a literal cannot contain the separator.
    2. No two declared topics can match the same wire topic. The manifest checks this
       property when it compiles.
    3. A param value is not empty and does not contain the separator. This module checks
       this property on each call.

  Each property is necessary. `["a", :x]` and `["a", :x, "b"]` have different segment
  counts, so (2) holds. But `x = "1:b"` on the first topic builds `"a:1:b"`, and the second
  topic builds the same wire topic from `x = "1"`. Without (3), a broadcast reaches the
  subscribers of the other topic.
  """

  @separator ":"

  @doc "The character that separates topic segments."
  @spec separator() :: String.t()
  def separator, do: @separator

  @doc """
  Renders segments as a wire pattern, with each param in angle brackets.

      iex> VerifiedPubSub.Topic.pattern(["owners", :owner_id, "pets"])
      "owners:<owner_id>:pets"

  Errors show a topic in this form. A reader with a wire topic from a log can then find the
  declared topic that it belongs to.
  """
  @spec pattern([VerifiedPubSub.Dsl.Topic.segment()]) :: String.t()
  def pattern(segments) do
    Enum.map_join(segments, @separator, fn
      literal when is_binary(literal) -> literal
      param -> "<#{param}>"
    end)
  end

  @doc false
  # Names one topic in error text, as `:pets (owners:<owner_id>:pets)`.
  @spec describe(VerifiedPubSub.Dsl.Topic.t()) :: String.t()
  def describe(topic), do: "#{inspect(topic.name)} (#{pattern(topic.segments)})"

  @doc false
  # Lists each topic of a manifest on its own line, as `name  pattern  location`. The
  # columns align, and the lines sort by name. An error uses the list to show the
  # alternatives.
  @spec listing([VerifiedPubSub.Dsl.Topic.t()]) :: String.t()
  def listing(topics) do
    sorted = Enum.sort_by(topics, & &1.name)
    name_width = max_length(sorted, &inspect(&1.name))
    pattern_width = max_length(sorted, &pattern(&1.segments))

    Enum.map_join(sorted, "\n", fn topic ->
      line =
        "  " <>
          String.pad_trailing(inspect(topic.name), name_width) <>
          "   " <> String.pad_trailing(pattern(topic.segments), pattern_width)

      case location(topic) do
        nil -> String.trim_trailing(line)
        location -> line <> "   " <> location
      end
    end)
  end

  defp max_length(items, fun) do
    items
    |> Enum.map(&String.length(fun.(&1)))
    |> Enum.max(fn -> 0 end)
  end

  @doc false
  # Returns the place where a manifest declares a topic, message, or field, as `path:line`.
  # The path is relative to the working directory, so a reader can go from an error to the
  # declaration. Returns nil when Spark has no location for the entity.
  @spec location(map()) :: String.t() | nil
  def location(entity) do
    with anno when anno != nil <- anno(entity),
         file when file != :undefined <- :erl_anno.file(anno) do
      "#{Path.relative_to_cwd(to_string(file))}:#{:erl_anno.line(anno)}"
    else
      _ -> nil
    end
  end

  @doc false
  # Returns the source annotation that Spark keeps for a DSL entity, or nil.
  @spec anno(map()) :: :erl_anno.anno() | nil
  def anno(%{__spark_metadata__: %{anno: anno}}), do: anno
  def anno(_entity), do: nil

  @doc false
  # Returns a sentence that names where the manifest declares `entity`, or "" when the
  # location is unknown. The sentence starts with a newline, so it can end a line of error
  # text.
  @spec declared_at(String.t(), map()) :: String.t()
  def declared_at(subject, entity) do
    case location(entity) do
      nil -> ""
      location -> "\n#{subject} is declared at #{location}."
    end
  end

  @doc """
  Converts `value` to a string, and checks that it can fill one segment of the topic.

  Raises `VerifiedPubSub.TopicError` when it cannot.
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
