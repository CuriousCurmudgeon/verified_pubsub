defmodule VerifiedPubSub.Usage.Report do
  @moduledoc false

  # Joins the call sites that a list of modules records to the manifests that they bind.
  # The manifest gives the order of the topics and the events, and the events that no
  # call site uses.

  alias VerifiedPubSub.{Info, Usage}
  alias VerifiedPubSub.Usage.Entry

  defmodule Event do
    @moduledoc false

    @typedoc """
    One event of a topic:

    * `:name` - the name of the event
    * `:entries` - the call sites of the event, by kind, then by file and line
    """
    @type t :: %__MODULE__{name: atom(), entries: [Entry.t()]}

    @enforce_keys [:name, :entries]
    defstruct @enforce_keys
  end

  defmodule Topic do
    @moduledoc false

    @typedoc """
    One topic of a manifest:

    * `:name` - the name of the topic
    * `:wire` - the wire pattern of the topic, as `owners:<owner_id>:pets`
    * `:subscribes` - the `:subscribe` and `:unsubscribe` entries, by file and line
    * `:events` - the events of the topic, in the order of the manifest
    """
    @type t :: %__MODULE__{
            name: atom(),
            wire: String.t(),
            subscribes: [Entry.t()],
            events: [Event.t()]
          }

    @enforce_keys [:name, :wire, :subscribes, :events]
    defstruct @enforce_keys
  end

  defmodule Manifest do
    @moduledoc false

    @typedoc """
    One manifest:

    * `:module` - the manifest module
    * `:topics` - the topics of the manifest, in the order of the manifest
    """
    @type t :: %__MODULE__{module: module(), topics: [Topic.t()]}

    @enforce_keys [:module, :topics]
    defstruct @enforce_keys
  end

  @typedoc """
  The view of a report:

  * `:overview` - each topic of each manifest
  * `:topic` - one topic
  * `:event` - one event of one topic
  """
  @type view :: :overview | :topic | :event

  @typedoc """
  A report:

  * `:view` - the view that the filters give
  * `:manifests` - the manifests, sorted by name
  """
  @type t :: %__MODULE__{view: view(), manifests: [Manifest.t()]}

  @typedoc """
  The number of entries of each kind:

  * `:broadcast` - the `:broadcast` and `:broadcast_from` entries
  * `:handle` - the `:handle` entries
  * `:ignore` - the `:ignore` entries
  * `:subscribe` - the `:subscribe` entries
  """
  @type counts :: %{
          broadcast: non_neg_integer(),
          handle: non_neg_integer(),
          ignore: non_neg_integer(),
          subscribe: non_neg_integer()
        }

  @enforce_keys [:view, :manifests]
  defstruct @enforce_keys

  @kinds [:broadcast, :broadcast_from, :handle, :ignore, :subscribe, :unsubscribe]

  @doc false
  @spec build([module()], keyword()) :: {:ok, t()} | {:error, String.t()}
  def build(modules, opts) do
    entries =
      modules
      |> Enum.flat_map(&Usage.entries/1)
      |> Enum.map(fn %Entry{} = entry ->
        %Entry{entry | file: Path.relative_to_cwd(entry.file)}
      end)

    with {:ok, manifests} <- build_manifests(modules, entries, opts[:manifest]),
         {:ok, manifests} <- filter_topic(manifests, opts[:topic]),
         {:ok, manifests} <- filter_event(manifests, opts[:event]) do
      {:ok, %__MODULE__{view: view(opts), manifests: manifests}}
    end
  end

  defp build_manifests(modules, entries, name) do
    modules
    |> Enum.filter(&manifest?/1)
    |> Enum.concat(Enum.map(entries, & &1.manifest))
    |> Enum.uniq()
    |> Enum.sort_by(&inspect/1)
    |> select_manifests(name)
    |> case do
      {:ok, manifests} -> {:ok, Enum.map(manifests, &build_manifest(&1, entries))}
      error -> error
    end
  end

  defp manifest?(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__verified_pubsub_name__, 0)
  end

  defp select_manifests(manifests, nil), do: {:ok, manifests}

  defp select_manifests(manifests, name) do
    case Enum.filter(manifests, &(inspect(&1) == name)) do
      [] ->
        {:error,
         "unknown manifest #{name}.\n\nManifests:\n" <>
           Enum.map_join(manifests, "\n", &("  " <> inspect(&1)))}

      selected ->
        {:ok, selected}
    end
  end

  defp build_manifest(module, entries) do
    topics =
      module
      |> Info.topics()
      |> Enum.map(fn topic ->
        build_topic(
          topic,
          Enum.filter(entries, &(&1.manifest == module and &1.topic == topic.name))
        )
      end)

    %Manifest{module: module, topics: topics}
  end

  defp build_topic(topic, entries) do
    {subscribes, calls} = Enum.split_with(entries, &(&1.kind in [:subscribe, :unsubscribe]))

    events =
      Enum.map(topic.messages, fn message ->
        event_entries =
          calls
          |> Enum.filter(&(&1.event == message.name))
          |> Enum.sort_by(&{kind_rank(&1.kind), &1.file, &1.line})

        %Event{name: message.name, entries: event_entries}
      end)

    %Topic{
      name: topic.name,
      wire: VerifiedPubSub.Topic.pattern(topic.segments),
      subscribes: Enum.sort_by(subscribes, &{&1.file, &1.line}),
      events: events
    }
  end

  defp kind_rank(kind), do: Enum.find_index(@kinds, &(&1 == kind))

  defp filter_topic(manifests, nil), do: {:ok, manifests}

  defp filter_topic(manifests, name) do
    name = bare_name(name)

    manifests
    |> Enum.map(fn %Manifest{} = manifest ->
      %Manifest{
        manifest
        | topics: Enum.filter(manifest.topics, &(Atom.to_string(&1.name) == name))
      }
    end)
    |> Enum.reject(&(&1.topics == []))
    |> case do
      [] -> {:error, unknown_topic_message(manifests, name)}
      selected -> {:ok, selected}
    end
  end

  # A caller can write a name with its colon, as `:pets`, or without it.
  defp bare_name(":" <> name), do: name
  defp bare_name(name), do: name

  defp unknown_topic_message(manifests, name) do
    listings =
      Enum.map_join(manifests, "\n\n", fn manifest ->
        "Declared topics in #{inspect(manifest.module)}:\n" <>
          VerifiedPubSub.Topic.listing(Info.topics(manifest.module))
      end)

    "unknown topic :#{name}.\n\n" <> listings
  end

  defp filter_event(manifests, nil), do: {:ok, manifests}

  defp filter_event(manifests, name) do
    name = bare_name(name)

    manifests
    |> Enum.map(fn %Manifest{} = manifest ->
      topics =
        manifest.topics
        |> Enum.map(&keep_event(&1, name))
        |> Enum.reject(&(&1.events == []))

      %Manifest{manifest | topics: topics}
    end)
    |> Enum.reject(&(&1.topics == []))
    |> case do
      [] -> {:error, unknown_event_message(manifests, name)}
      selected -> {:ok, selected}
    end
  end

  defp keep_event(%Topic{} = topic, name) do
    %Topic{topic | events: Enum.filter(topic.events, &(Atom.to_string(&1.name) == name))}
  end

  defp unknown_event_message(manifests, name) do
    manifests
    |> Enum.flat_map(& &1.topics)
    |> Enum.map_join("\n\n", fn topic ->
      "unknown event :#{name} on topic #{inspect(topic.name)} (#{topic.wire}).\n\n" <>
        "Declared events: #{inspect(Enum.map(topic.events, & &1.name))}"
    end)
  end

  defp view(opts) do
    cond do
      opts[:event] -> :event
      opts[:topic] -> :topic
      true -> :overview
    end
  end

  @doc false
  @spec counts([Entry.t()]) :: counts()
  def counts(entries) do
    Enum.reduce(entries, %{broadcast: 0, handle: 0, ignore: 0, subscribe: 0}, fn entry, counts ->
      case count_key(entry.kind) do
        nil -> counts
        key -> Map.update!(counts, key, &(&1 + 1))
      end
    end)
  end

  defp count_key(:broadcast_from), do: :broadcast
  defp count_key(:unsubscribe), do: nil
  defp count_key(kind), do: kind
end
