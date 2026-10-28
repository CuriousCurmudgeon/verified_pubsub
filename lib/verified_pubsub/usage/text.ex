defmodule VerifiedPubSub.Usage.Text do
  @moduledoc false

  alias VerifiedPubSub.Usage.{Entry, Report}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  @headers ["Topic", "Wire topic", "Events", "Broadcasts", "Handlers", "Subscribes"]

  # The longest macro name is `broadcast_from`, `handle_message`, or `ignore_message`.
  @macro_width 15

  @doc false
  # Returns ANSI data, not a string. `Mix.shell().info/1` gives it color when ANSI is on,
  # and drops the codes when ANSI is off, as in a pipe.
  @spec render(Report.t()) :: IO.ANSI.ansidata()
  def render(%Report{manifests: []}) do
    "No VerifiedPubSub manifest is in this project."
  end

  def render(%Report{view: :summary, manifests: manifests}) do
    manifests
    |> Enum.map(&render_summary_manifest/1)
    |> Enum.intersperse("\n\n")
  end

  def render(%Report{manifests: manifests}) do
    width =
      manifests
      |> Enum.flat_map(& &1.topics)
      |> Enum.flat_map(&topic_entries/1)
      |> Enum.map(&String.length(location(&1)))
      |> Enum.max(fn -> 0 end)

    manifests
    |> Enum.map(&render_detail_manifest(&1, width))
    |> Enum.intersperse("\n\n")
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

    [style(inspect(manifest.module), [:bright]), "\n\n", render_table(rows)]
  end

  defp topic_entries(%Topic{} = topic) do
    topic.subscribes ++ Enum.flat_map(topic.events, & &1.entries)
  end

  defp style(text, codes), do: codes ++ [text, :reset]

  defp render_table(rows) do
    [name_width, wire_width] =
      Enum.map(0..1, fn index ->
        [@headers | rows]
        |> Enum.map(&String.length(Enum.at(&1, index)))
        |> Enum.max()
      end)

    [name_header | cell_headers] = @headers

    header_text =
      String.pad_trailing(name_header, name_width) <>
        "  " <> render_cells(cell_headers, wire_width)

    header = style(header_text, [:bright])

    lines =
      Enum.map(rows, fn [name | cells] ->
        # The padding goes after the color, so that the color covers only the name.
        padding = String.duplicate(" ", name_width - String.length(name))
        [style(name, [:cyan]), padding, "  ", render_cells(cells, wire_width)]
      end)

    Enum.intersperse([header | lines], "\n")
  end

  # The cells after the name: the wire pattern, then each count under its header.
  defp render_cells([wire | numbers], wire_width) do
    cells =
      numbers
      |> Enum.zip(Enum.drop(@headers, 2))
      |> Enum.map(fn {number, header} ->
        number
        |> to_string()
        |> String.pad_leading(String.length(header))
      end)

    Enum.join([String.pad_trailing(wire, wire_width) | cells], "  ")
  end

  defp location(%Entry{} = entry), do: "#{entry.file}:#{entry.line}"

  defp render_detail_manifest(%Manifest{} = manifest, width) do
    topics = Enum.map(manifest.topics, &render_topic(&1, width))

    Enum.intersperse([style(inspect(manifest.module), [:bright]) | topics], "\n\n")
  end

  defp render_topic(%Topic{} = topic, width) do
    header = [style(inspect(topic.name), [:bright, :cyan]), "  ", topic.wire]
    subscribes = render_subscribes(topic.subscribes, width)
    events = Enum.map(topic.events, &render_event(&1, width))

    Enum.intersperse([header, subscribes | events], "\n\n")
  end

  defp render_subscribes([], _width), do: "Subscribes: none"

  defp render_subscribes(subscribes, width) do
    Enum.intersperse(
      ["Subscribes (#{length(subscribes)})" | Enum.map(subscribes, &render_entry(&1, width))],
      "\n"
    )
  end

  defp render_entry(%Entry{} = entry, width) do
    [
      "  ",
      style(String.pad_trailing(macro(entry.kind), @macro_width), [kind_color(entry.kind)]),
      "  ",
      String.pad_trailing(location(entry), width),
      "  ",
      inspect(entry.module)
    ]
  end

  defp macro(:handle), do: "handle_message"
  defp macro(:ignore), do: "ignore_message"
  defp macro(kind), do: Atom.to_string(kind)

  # A send and a receive get two colors, so that a reader tells them apart at a glance.
  defp kind_color(kind) when kind in [:broadcast, :broadcast_from], do: :green
  defp kind_color(kind) when kind in [:handle, :subscribe], do: :magenta
  defp kind_color(kind) when kind in [:ignore, :unsubscribe], do: :faint

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

    flag = if counts.broadcast == 0, do: ["  ", style("(no broadcast)", [:yellow])], else: []
    line = [style(inspect(event.name), [:bright]), "  ", header, flag]

    Enum.intersperse([line | Enum.map(event.entries, &render_entry(&1, width))], "\n")
  end

  defp count(1, noun), do: "1 #{noun}"
  defp count(number, noun), do: "#{number} #{noun}s"
end
