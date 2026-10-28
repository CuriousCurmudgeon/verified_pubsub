defmodule VerifiedPubSub.Usage.Text do
  @moduledoc false

  alias VerifiedPubSub.Usage.{Entry, Report}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  @headers ["Topic", "Wire topic", "Events", "Broadcasts", "Handlers", "Subscribes"]

  # The longest macro name is `broadcast_from`, `handle_message`, or `ignore_message`.
  @macro_width 15

  @doc false
  @spec render(Report.t()) :: String.t()
  def render(%Report{manifests: []}) do
    "No VerifiedPubSub manifest is in this project."
  end

  def render(%Report{view: :summary, manifests: manifests}) do
    Enum.map_join(manifests, "\n\n", &render_summary_manifest/1)
  end

  def render(%Report{manifests: manifests}) do
    width =
      manifests
      |> Enum.flat_map(& &1.topics)
      |> Enum.flat_map(&topic_entries/1)
      |> Enum.map(&String.length(location(&1)))
      |> Enum.max(fn -> 0 end)

    Enum.map_join(manifests, "\n\n", &render_detail_manifest(&1, width))
  end

  defp render_summary_manifest(%Manifest{} = manifest) do
    rows =
      Enum.map(manifest.topics, fn topic ->
        counts = Report.counts(topic_entries(topic))

        [
          inspect(topic.name),
          topic.wire,
          length(topic.events),
          counts.broadcast,
          counts.handle,
          counts.subscribe
        ]
      end)

    inspect(manifest.module) <> "\n\n" <> render_table(rows)
  end

  defp topic_entries(%Topic{} = topic) do
    topic.subscribes ++ Enum.flat_map(topic.events, & &1.entries)
  end

  defp render_table(rows) do
    [name_width, wire_width] =
      Enum.map(0..1, fn index ->
        [@headers | rows]
        |> Enum.map(&String.length(Enum.at(&1, index)))
        |> Enum.max()
      end)

    [@headers | rows]
    |> Enum.map_join("\n", fn [name, wire | numbers] ->
      cells =
        numbers
        |> Enum.zip(Enum.drop(@headers, 2))
        |> Enum.map(fn {number, header} ->
          number
          |> to_string()
          |> String.pad_leading(String.length(header))
        end)

      Enum.join(
        [String.pad_trailing(name, name_width), String.pad_trailing(wire, wire_width) | cells],
        "  "
      )
    end)
  end

  defp location(%Entry{} = entry), do: "#{entry.file}:#{entry.line}"

  defp render_detail_manifest(%Manifest{} = manifest, width) do
    topics = Enum.map(manifest.topics, &render_topic(&1, width))

    Enum.join([inspect(manifest.module) | topics], "\n\n")
  end

  defp render_topic(%Topic{} = topic, width) do
    header = "#{inspect(topic.name)}  #{topic.wire}"
    subscribes = render_subscribes(topic.subscribes, width)
    events = Enum.map(topic.events, &render_event(&1, width))

    Enum.join([header, subscribes | events], "\n\n")
  end

  defp render_subscribes([], _width), do: "Subscribes: none"

  defp render_subscribes(subscribes, width) do
    Enum.join(
      ["Subscribes (#{length(subscribes)})" | Enum.map(subscribes, &render_entry(&1, width))],
      "\n"
    )
  end

  defp render_entry(%Entry{} = entry, width) do
    "  " <>
      String.pad_trailing(macro(entry.kind), @macro_width) <>
      "  " <> String.pad_trailing(location(entry), width) <> "  " <> inspect(entry.module)
  end

  defp macro(:handle), do: "handle_message"
  defp macro(:ignore), do: "ignore_message"
  defp macro(kind), do: Atom.to_string(kind)

  defp render_event(%Event{} = event, width) do
    counts = Report.counts(event.entries)

    header =
      [
        count(counts.broadcast, "broadcast"),
        counts.handle > 0 && count(counts.handle, "handler"),
        counts.ignore > 0 && count(counts.ignore, "ignore")
      ]
      |> Enum.filter(& &1)
      |> Enum.join(", ")

    flag = if counts.broadcast == 0, do: "  (no broadcast)", else: ""

    Enum.join(
      [
        "#{inspect(event.name)}  #{header}#{flag}"
        | Enum.map(event.entries, &render_entry(&1, width))
      ],
      "\n"
    )
  end

  defp count(1, noun), do: "1 #{noun}"
  defp count(number, noun), do: "#{number} #{noun}s"
end
