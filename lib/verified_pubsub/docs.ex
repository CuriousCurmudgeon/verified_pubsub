defmodule VerifiedPubSub.Docs do
  @moduledoc false

  # Renders the `## Topics` section of the @moduledoc of a manifest, and the `## PubSub`
  # section of a module that uses the manifest. The declarations and the macro calls give
  # each section, so a section always agrees with what the code does.

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

  @doc false
  # Adds the `## PubSub` section to the end of the moduledoc of `module`, when docs are on.
  # A module can run this more than once. The first run saves the moduledoc of the user,
  # and each run builds the result from that saved value, so the section appears once. The
  # last run sees the most macro calls, so its section wins. A module with no moduledoc
  # gets `line`, the line of its `defmodule`, as Elixir gives it without this section.
  @spec put_moduledoc(module(), pos_integer()) :: :ok
  def put_moduledoc(module, line) do
    if docs?(module) do
      case moduledoc(user_moduledoc(module), pubsub_section(module), line) do
        nil -> :ok
        moduledoc -> Module.put_attribute(module, :moduledoc, moduledoc)
      end
    end

    :ok
  end

  # The `docs` option of a `use` line wins. Without one, the manifest gives the default.
  defp docs?(module) do
    case Module.get_attribute(module, :verified_pubsub_docs) do
      nil -> Module.get_attribute(module, :verified_pubsub_manifest).__verified_pubsub_docs__()
      docs -> docs
    end
  end

  defp moduledoc(user_moduledoc, nil, _module_line), do: user_moduledoc
  defp moduledoc({_, false} = user_moduledoc, _section, _module_line), do: user_moduledoc
  defp moduledoc(nil, section, line), do: {line, section}

  defp moduledoc({line, doc}, section, _module_line) do
    {line, String.trim_trailing(doc) <> "\n\n" <> section}
  end

  defp user_moduledoc(module) do
    if Module.has_attribute?(module, :verified_pubsub_user_moduledoc) do
      Module.get_attribute(module, :verified_pubsub_user_moduledoc)
    else
      user_moduledoc = Module.get_attribute(module, :moduledoc)
      Module.put_attribute(module, :verified_pubsub_user_moduledoc, user_moduledoc)
      user_moduledoc
    end
  end

  # One row for each topic that the module publishes to or handles. The rows and the events
  # in each cell follow the order of the manifest, so a reader finds them where the manifest
  # puts them.
  defp pubsub_section(module) do
    manifest = Module.get_attribute(module, :verified_pubsub_manifest)

    published =
      module
      |> Module.get_attribute(:verified_pubsub_broadcasts)
      |> MapSet.new()

    handled = MapSet.new(handled_topic_events(module))

    manifest
    |> VerifiedPubSub.Info.topics()
    |> Enum.map(&row(&1, published, handled))
    |> Enum.reject(&is_nil/1)
    |> case do
      [] ->
        nil

      rows ->
        table = ["| Topic | Publishes | Subscribes |", "| --- | --- | --- |" | rows]
        "## PubSub\n\n" <> Enum.join(table, "\n")
    end
  end

  # A subscriber's handlers say which events it receives, so an ignored event never
  # appears. A module without the Subscriber has its own handle_info/2, and the library
  # does not know which of its messages it handles, so it lists none.
  defp handled_topic_events(module) do
    if Module.has_attribute?(module, :verified_pubsub_clauses) do
      module
      |> Module.get_attribute(:verified_pubsub_clauses)
      |> Enum.flat_map(fn clause -> Enum.map(clause.events, &{clause.topic, &1}) end)
    else
      []
    end
  end

  defp row(%Topic{} = topic, published, handled) do
    case {cell(topic, published), cell(topic, handled)} do
      {"", ""} ->
        nil

      {publishes, subscribes} ->
        name =
          "`#{inspect(topic.name)}` (`#{inspect(VerifiedPubSub.Topic.pattern(topic.segments))}`)"

        "|" <> Enum.map_join([name, publishes, subscribes], "|", &pad/1) <> "|"
    end
  end

  defp cell(topic, topic_events) do
    topic.messages
    |> Enum.filter(&MapSet.member?(topic_events, {topic.name, &1.name}))
    |> Enum.map_join(", ", &"`#{inspect(&1.name)}`")
  end

  defp pad(""), do: " "
  defp pad(text), do: " " <> text <> " "
end
