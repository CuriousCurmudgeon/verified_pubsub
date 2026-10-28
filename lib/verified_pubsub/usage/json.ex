defmodule VerifiedPubSub.Usage.Json do
  @moduledoc false

  alias VerifiedPubSub.Usage.{Entry, Report}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  @doc false
  @spec render(Report.t()) :: {:ok, String.t()} | {:error, String.t()}
  def render(%Report{} = report) do
    if supported?(System.version()) do
      # The library supports Elixir 1.16, which has no JSON module. A call through a
      # variable is not a remote call that the compiler checks, so 1.16 gives no warning.
      json = JSON
      document = object([{"manifests", Enum.map(report.manifests, &manifest(&1, report.view))}])

      {:ok, IO.iodata_to_binary(json.encode_to_iodata!(document, &encode/2))}
    else
      {:error,
       "--format json needs Elixir 1.18 or later, because it uses the JSON module of Elixir."}
    end
  end

  # A map of at most 32 keys sorts its keys, so a map gives `name` after `events`. An
  # object keeps its keys in the order of the list, with the name first.
  defp object(pairs), do: {:object, pairs}

  defp manifest(%Manifest{} = manifest, view) do
    object([
      {"module", inspect(manifest.module)},
      {"topics", Enum.map(manifest.topics, &topic(&1, view))}
    ])
  end

  defp topic(%Topic{} = topic, view) do
    entries = topic.subscribes ++ Enum.flat_map(topic.events, & &1.entries)

    object(
      [
        {"name", Atom.to_string(topic.name)},
        {"wire", topic.wire},
        {"counts", counts(entries, [:broadcast, :handle, :ignore, :subscribe])}
      ] ++
        detail(view, "subscribes", topic.subscribes) ++
        [{"events", Enum.map(topic.events, &event(&1, view))}]
    )
  end

  defp counts(entries, keys) do
    counts = Report.counts(entries)

    keys
    |> Enum.map(&{Atom.to_string(&1), Map.fetch!(counts, &1)})
    |> object()
  end

  # The summary holds only the counts.
  defp detail(:summary, _key, _entries), do: []
  defp detail(_view, key, entries), do: [{key, Enum.map(entries, &call_site/1)}]

  defp call_site(%Entry{} = entry) do
    object([
      {"kind", Atom.to_string(entry.kind)},
      {"file", entry.file},
      {"line", entry.line},
      {"module", inspect(entry.module)}
    ])
  end

  # A subscribe names a topic and no event, so an event has no subscribe count. A count of
  # 0 would say that no process receives the event.
  defp event(%Event{} = event, view) do
    object(
      [
        {"name", Atom.to_string(event.name)},
        {"counts", counts(event.entries, [:broadcast, :handle, :ignore])}
      ] ++ detail(view, "call_sites", event.entries)
    )
  end

  # `JSON.encode_to_iodata!/2` calls this function for each value, and for each value
  # inside it.
  defp encode({:object, pairs}, encoder) do
    members =
      Enum.map_intersperse(pairs, ",", fn {key, value} ->
        [encoder.(key, encoder), ":", encoder.(value, encoder)]
      end)

    ["{", members, "}"]
  end

  defp encode(term, encoder) do
    json = JSON
    json.protocol_encode(term, encoder)
  end

  @doc false
  # The JSON module of Elixir arrives in 1.18. Before that, a module with the name `JSON`
  # can only come from a dependency, such as the `json` Hex package, with another API. So
  # the check reads the version of Elixir, not whether a `JSON` module loads.
  @spec supported?(String.t()) :: boolean()
  def supported?(version) do
    # A release candidate of 1.18 has the module, but semver orders it before 1.18.0.
    %Version{major: major, minor: minor} = Version.parse!(version)
    {major, minor} >= {1, 18}
  end
end
