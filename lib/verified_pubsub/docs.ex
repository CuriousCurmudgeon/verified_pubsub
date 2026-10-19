defmodule VerifiedPubSub.Docs do
  @moduledoc false

  # Renders the `## Topics` section of the @moduledoc of a manifest. All of the section
  # except the descriptions comes from the declarations, so the section always agrees with
  # what the manifest enforces.

  alias VerifiedPubSub.Dsl.{Field, Message, Topic}

  @doc false
  @spec topics_section([Topic.t(), ...]) :: String.t()
  def topics_section(topics) do
    Enum.join(["## Topics" | Enum.map(topics, &topic/1)], "\n\n")
  end

  # A topic always has one event or more, because
  # VerifiedPubSub.Transformers.ValidateTopics rejects a topic with no event.
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

  defp item(head, nil, _indent), do: head
  defp item(head, description, indent), do: head <> " - " <> indent_lines(description, indent)

  # A description can have many lines. Each line after the first gets an indent, so that
  # it stays inside the list item. A blank line stays blank, so that it has no trailing
  # whitespace.
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

  defp field(%Field{} = field) do
    item("* `#{inspect(field.name)}` (#{type_label(field)})", field.description, "    ")
  end

  defp type_label(%Field{type: type, required: true}), do: "`#{inspect(type)}`"
  defp type_label(%Field{type: type, required: false}), do: "`#{inspect(type)}`, optional"
end
