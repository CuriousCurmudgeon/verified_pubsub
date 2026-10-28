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

      {:ok,
       json.encode!(%{"manifests" => Enum.map(report.manifests, &manifest(&1, report.view))})}
    else
      {:error,
       "--format json needs Elixir 1.18 or later, because it uses the JSON module of Elixir."}
    end
  end

  defp manifest(%Manifest{} = manifest, view) do
    %{
      "module" => inspect(manifest.module),
      "topics" => Enum.map(manifest.topics, &topic(&1, view))
    }
  end

  defp topic(%Topic{} = topic, view) do
    entries = topic.subscribes ++ Enum.flat_map(topic.events, & &1.entries)

    %{
      "name" => Atom.to_string(topic.name),
      "wire" => topic.wire,
      "counts" => counts(entries),
      "events" => Enum.map(topic.events, &event(&1, view))
    }
    |> put_detail(view, "subscribes", topic.subscribes)
  end

  # A subscribe names a topic and no event, so an event has no subscribe count. A count of
  # 0 would say that no process receives the event.
  defp event(%Event{} = event, view) do
    counts =
      event.entries
      |> counts()
      |> Map.delete("subscribe")

    %{"name" => Atom.to_string(event.name), "counts" => counts}
    |> put_detail(view, "call_sites", event.entries)
  end

  defp counts(entries) do
    entries
    |> Report.counts()
    |> Map.new(fn {key, count} -> {Atom.to_string(key), count} end)
  end

  # The summary holds only the counts.
  defp put_detail(map, :summary, _key, _entries), do: map

  defp put_detail(map, _view, key, entries) do
    Map.put(map, key, Enum.map(entries, &call_site/1))
  end

  defp call_site(%Entry{} = entry) do
    %{
      "kind" => Atom.to_string(entry.kind),
      "module" => inspect(entry.module),
      "file" => entry.file,
      "line" => entry.line
    }
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
