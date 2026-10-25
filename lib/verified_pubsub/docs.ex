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
  # last run sees the most macro calls, so its section wins.
  @spec put_moduledoc(module()) :: :ok
  def put_moduledoc(module) do
    if docs?(module) do
      case moduledoc(user_moduledoc(module), pubsub_section(module)) do
        nil -> :ok
        moduledoc -> Module.put_attribute(module, :moduledoc, moduledoc)
      end
    end

    :ok
  end

  defp docs?(module), do: Module.get_attribute(module, :verified_pubsub_docs) == true

  defp moduledoc(user_moduledoc, nil), do: user_moduledoc
  defp moduledoc({_line, false} = user_moduledoc, _section), do: user_moduledoc
  defp moduledoc(nil, section), do: {1, section}

  defp moduledoc({line, doc}, section) do
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

  defp pubsub_section(module) do
    manifest = Module.get_attribute(module, :verified_pubsub_manifest)

    published =
      module
      |> Module.get_attribute(:verified_pubsub_broadcasts)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    bullets =
      bullets(manifest, "Publishes to", published) ++
        bullets(manifest, "Subscribes to", subscribed_topic_events(module))

    case bullets do
      [] -> nil
      bullets -> "## PubSub\n\n" <> Enum.join(bullets, "\n")
    end
  end

  defp bullets(manifest, verb, topic_events) do
    topic_events
    |> Enum.map(fn {topic, events} -> {wire_pattern(manifest, topic), events} end)
    |> Enum.sort()
    |> Enum.map(fn {pattern, events} -> bullet(verb, pattern, events) end)
  end

  defp wire_pattern(manifest, topic) do
    topic_struct = VerifiedPubSub.Info.topic!(manifest, topic)
    inspect(VerifiedPubSub.Topic.pattern(topic_struct.segments))
  end

  defp bullet(verb, pattern, []), do: "  * #{verb} `#{pattern}`."
  defp bullet(verb, pattern, events), do: "  * #{verb} `#{pattern}` for #{event_list(events)}."

  defp event_list(events) do
    events
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.map(&"`#{inspect(&1)}`")
    |> case do
      [event] ->
        event

      [first, second] ->
        "#{first} and #{second}"

      events ->
        {init, [last]} = Enum.split(events, -1)
        Enum.join(init, ", ") <> ", and " <> last
    end
  end

  # In a subscriber, the handlers say which events the module receives, so the handlers
  # give the topics, and an ignored event never appears. A module without the Subscriber
  # has its own handle_info/2, so only its subscribe calls show its topics, with no events.
  defp subscribed_topic_events(module) do
    if Module.has_attribute?(module, :verified_pubsub_clauses) do
      module
      |> Module.get_attribute(:verified_pubsub_clauses)
      |> Enum.flat_map(fn clause -> Enum.map(clause.events, &{clause.topic, &1}) end)
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    else
      module
      |> Module.get_attribute(:verified_pubsub_subscriptions)
      |> Map.new(&{&1, []})
    end
  end
end
