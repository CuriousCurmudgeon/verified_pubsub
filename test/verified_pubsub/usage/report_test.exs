defmodule VerifiedPubSub.Usage.ReportTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Info
  alias VerifiedPubSub.TestManifests.{Basic, CollideA, CollideB}
  alias VerifiedPubSub.Usage.{Entry, Report}

  setup do
    %{caller: compile_caller()}
  end

  describe "build/2" do
    test "lists each manifest with its topics and events in the order of the manifest", %{
      caller: caller
    } do
      assert {:ok, %Report{view: :overview, manifests: [manifest]}} =
               Report.build([Basic, caller], [])

      assert manifest.module == Basic
      assert Enum.map(manifest.topics, & &1.name) == Enum.map(Info.topics(Basic), & &1.name)

      [pets | _] = manifest.topics
      assert Enum.map(pets.events, & &1.name) == [:adopted, :renamed, :rehomed]
    end

    test "gives each topic its wire pattern", %{caller: caller} do
      assert {:ok, %Report{manifests: [manifest]}} = Report.build([Basic, caller], [])
      assert hd(manifest.topics).wire == "owners:<owner_id>:pets"
    end

    test "puts the subscribe and unsubscribe entries in the subscribes of the topic", %{
      caller: caller
    } do
      assert {:ok, report} = Report.build([Basic, caller], [])

      assert Enum.map(pets(report).subscribes, &{&1.kind, &1.line}) == [
               {:subscribe, 3},
               {:unsubscribe, 4}
             ]
    end

    test "orders the entries of an event by broadcast, broadcast_from, handle, and ignore, then by file and line",
         %{caller: caller} do
      assert {:ok, report} = Report.build([Basic, caller], [])
      [adopted, renamed, _rehomed] = pets(report).events

      assert Enum.map(adopted.entries, &{&1.kind, &1.line}) == [
               {:broadcast, 5},
               {:broadcast, 7},
               {:broadcast_from, 6},
               {:handle, 8}
             ]

      assert Enum.map(renamed.entries, &{&1.kind, &1.line}) == [{:ignore, 11}]
    end

    test "makes each file relative to the current directory", %{caller: caller} do
      assert {:ok, report} = Report.build([Basic, caller], [])
      assert Enum.all?(pets(report).subscribes, &(&1.file == "lib/fake.ex"))
    end

    test "reports a manifest that only an entry names", %{caller: caller} do
      assert {:ok, %Report{manifests: [manifest]}} = Report.build([caller], [])
      assert manifest.module == Basic
    end

    test "leaves out a manifest that no module of the list defines and no entry names", %{
      caller: caller
    } do
      assert {:ok, %Report{manifests: [manifest]}} = Report.build([caller, Enum], [])
      assert manifest.module == Basic
    end
  end

  describe "build/2 with a topic" do
    test "keeps only the topic, in the :topic view", %{caller: caller} do
      assert {:ok, %Report{view: :topic, manifests: [manifest]}} =
               Report.build([Basic, caller], topic: "pets")

      assert Enum.map(manifest.topics, & &1.name) == [:pets]
    end

    test "accepts the topic with a leading colon", %{caller: caller} do
      assert Report.build([Basic, caller], topic: ":pets") ==
               Report.build([Basic, caller], topic: "pets")
    end

    test "keeps the topic of each manifest that declares it" do
      assert {:ok, %Report{manifests: manifests}} =
               Report.build([Basic, CollideA, CollideB], topic: "pets")

      assert Enum.map(manifests, & &1.module) == [Basic, CollideA, CollideB]
      assert Enum.all?(manifests, &match?([%Report.Topic{name: :pets}], &1.topics))
    end

    test "returns an error that says no manifest exists when the list has no manifest" do
      assert Report.build([Enum], topic: "pets") ==
               {:error, "unknown topic :pets. No VerifiedPubSub manifest is in this project."}
    end

    test "returns an error that lists the declared topics for an unknown topic" do
      assert {:error, message} = Report.build([Basic], topic: "petz")
      assert message =~ "unknown topic :petz"
      assert message =~ VerifiedPubSub.Topic.listing(Info.topics(Basic))
    end
  end

  describe "build/2 with a topic and an event" do
    test "keeps only the event and the subscribes of the topic, in the :event view", %{
      caller: caller
    } do
      assert {:ok, %Report{view: :event} = report} =
               Report.build([Basic, caller], topic: "pets", event: "adopted")

      topic = pets(report)
      assert Enum.map(topic.events, & &1.name) == [:adopted]
      assert length(topic.subscribes) == 2
    end

    test "keeps the event of the named topic when another topic declares an event with the same name" do
      manifest =
        compile!("""
        defmodule #{unique_module("VPTest.ReportManifest")} do
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

          topic :one, ["one"] do
            message :ping
          end

          topic :two, ["two"] do
            message :ping
          end
        end
        """)

      assert {:ok, %Report{manifests: [%Report.Manifest{topics: [topic]}]}} =
               Report.build([manifest], topic: "two", event: "ping")

      assert topic.name == :two
      assert Enum.map(topic.events, & &1.name) == [:ping]
    end

    test "returns an error that lists the events of the topic for an unknown event" do
      assert {:error, message} = Report.build([Basic], topic: "pets", event: "adoptd")
      assert message =~ "unknown event :adoptd on topic :pets (owners:<owner_id>:pets)"
      assert message =~ ":adopted"
      assert message =~ ":renamed"
      assert message =~ ":rehomed"
    end
  end

  describe "build/2 with a manifest" do
    test "keeps only the named manifest" do
      assert {:ok, %Report{manifests: [manifest]}} =
               Report.build([Basic, CollideA], manifest: "VerifiedPubSub.TestManifests.Basic")

      assert manifest.module == Basic
    end

    test "returns an error that says no manifest exists when the list has no manifest" do
      assert Report.build([Enum], manifest: "MyApp.Topics") ==
               {:error,
                "unknown manifest MyApp.Topics. No VerifiedPubSub manifest is in this project."}
    end

    test "returns an error that lists the manifests for an unknown manifest" do
      assert {:error, message} = Report.build([Basic, CollideA], manifest: "Nope.Topics")
      assert message =~ "unknown manifest Nope.Topics"
      assert message =~ "VerifiedPubSub.TestManifests.Basic"
      assert message =~ "VerifiedPubSub.TestManifests.CollideA"
    end
  end

  describe "counts/1" do
    test "counts broadcast and broadcast_from as broadcasts" do
      entries = Enum.map([:broadcast, :broadcast_from, :handle, :subscribe], &entry/1)

      assert Report.counts(entries) == %{broadcast: 2, handle: 1, ignore: 0, subscribe: 1}
    end
  end

  defp pets(%Report{manifests: [manifest | _]}) do
    Enum.find(manifest.topics, &(&1.name == :pets))
  end

  defp entry(kind) do
    %Entry{
      manifest: Basic,
      topic: :pets,
      event: :adopted,
      kind: kind,
      module: Basic,
      file: "lib/fake.ex",
      line: 1
    }
  end

  # The line numbers in the tests above follow this source.
  defp compile_caller do
    source = """
    defmodule #{unique_module("VPTest.ReportCaller")} do
      use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic
      def s, do: subscribe(:pets, %{owner_id: "1"})
      def u, do: unsubscribe(:pets, %{owner_id: "1"})
      def b, do: broadcast!(:pets, %{owner_id: "1"}, :adopted, %{id: "1"})
      def f, do: broadcast_from!(self(), :pets, %{owner_id: "1"}, :adopted, %{id: "1"})
      def c, do: broadcast!(:pets, %{owner_id: "1"}, :adopted, %{id: "1"})
      handle_message :pets, :adopted, _payload, state do
        {:noreply, state}
      end
      ignore_message :pets, [:renamed, :rehomed]
    end
    """

    [{module, _bin} | _] = Code.compile_string(source, Path.join(File.cwd!(), "lib/fake.ex"))
    module
  end
end
