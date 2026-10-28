defmodule VerifiedPubSub.Usage.Json do
  @moduledoc false

  alias VerifiedPubSub.Usage.{Entry, Report}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  @doc false
  @spec render(Report.t()) :: {:ok, String.t()} | {:error, String.t()}
  def render(%Report{} = report) do
    if Code.ensure_loaded?(JSON) do
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

  defp event(%Event{} = event, view) do
    %{"name" => Atom.to_string(event.name), "counts" => counts(event.entries)}
    |> put_detail(view, "call_sites", event.entries)
  end

  defp counts(entries) do
    entries
    |> Report.counts()
    |> Map.new(fn {key, count} -> {Atom.to_string(key), count} end)
  end

  # The overview holds only the counts. A caller asks for one topic to get the call sites.
  defp put_detail(map, :overview, _key, _entries), do: map

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
end
