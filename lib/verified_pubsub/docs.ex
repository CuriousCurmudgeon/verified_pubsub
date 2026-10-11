defmodule VerifiedPubSub.Docs do
  @moduledoc false

  # Renders the `## Topics` section that VerifiedPubSub.Dsl.explain/1 hands to Spark for
  # the manifest's @moduledoc. Everything except the descriptions is derived from the
  # declarations, so the section cannot drift from what the manifest enforces.

  alias VerifiedPubSub.Dsl.{Field, Message, Topic}

  @doc false
  @spec topics_section([Topic.t(), ...]) :: String.t()
  def topics_section(topics) do
    Enum.join(["## Topics" | Enum.map(topics, &topic/1)], "\n\n")
  end

  # A topic always has at least one event: VerifiedPubSub.Transformers.ValidateTopics
  # rejects a topic without one.
  defp topic(%Topic{} = topic) do
    [
      "### `#{inspect(topic.name)}`",
      topic.description && String.trim(topic.description),
      "Wire topic: `#{inspect(VerifiedPubSub.Topic.pattern(topic.segments))}`",
      "Events:",
      Enum.map_join(topic.messages, "\n", &message/1)
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n\n")
  end

  defp message(%Message{} = message) do
    item("* `#{inspect(message.name)}`", message.description, "  ") <>
      Enum.map_join(message.fields, "", &("\n  " <> field(&1)))
  end

  defp field(%Field{} = field) do
    item("* `#{inspect(field.name)}` (#{type_label(field)})", field.description, "    ")
  end

  defp type_label(%Field{type: type, required: true}), do: "`#{inspect(type)}`"
  defp type_label(%Field{type: type, required: false}), do: "`#{inspect(type)}`, optional"

  defp item(head, nil, _indent), do: head
  defp item(head, description, indent), do: head <> " - " <> indent_lines(description, indent)

  # A description can span lines. Continuation lines are indented to sit inside the list
  # item, and blank lines stay blank so they carry no trailing whitespace.
  defp indent_lines(text, indent) do
    text
    |> String.trim()
    |> String.split("\n")
    |> Enum.map_join("\n", fn
      "" -> ""
      line -> indent <> line
    end)
    |> String.trim_leading()
  end
end
