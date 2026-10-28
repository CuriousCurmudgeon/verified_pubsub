defmodule VerifiedPubSub.Usage.JsonTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Usage.{Entry, Json, Report}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  # The JSON module arrives in Elixir 1.18, and test_helper.exs excludes this tag before.
  @moduletag :json

  describe "render/1" do
    test "renders the summary with counts and no call sites" do
      assert decode(report(:summary)) == %{
               "manifests" => [
                 %{
                   "module" => "MyApp.Topics",
                   "topics" => [
                     %{
                       "name" => "pets",
                       "wire" => "owners:<owner_id>:pets",
                       "counts" => %{
                         "broadcast" => 2,
                         "handle" => 1,
                         "ignore" => 0,
                         "subscribe" => 1
                       },
                       "events" => [
                         %{
                           "name" => "adopted",
                           "counts" => %{
                             "broadcast" => 2,
                             "handle" => 1,
                             "ignore" => 0
                           }
                         }
                       ]
                     }
                   ]
                 }
               ]
             }
    end

    test "renders the detail with the subscribes and the call sites of each event" do
      assert %{"manifests" => [%{"topics" => [topic]}]} = decode(report(:detail))

      assert topic["subscribes"] == [
               %{
                 "kind" => "subscribe",
                 "module" => "MyApp.Notifier",
                 "file" => "lib/my_app/notifier.ex",
                 "line" => 22
               }
             ]

      assert [%{"name" => "adopted", "counts" => %{"broadcast" => 2}, "call_sites" => sites}] =
               topic["events"]

      assert Enum.map(sites, &{&1["kind"], &1["line"]}) == [
               {"broadcast", 12},
               {"broadcast_from", 51},
               {"handle", 30}
             ]
    end

    test "writes the keys of each object in a fixed order, with the name first" do
      assert {:ok, json} = Json.render(report(:detail))

      assert json =~ ~s({"manifests":[{"module":"MyApp.Topics","topics":[)
      assert json =~ ~s({"name":"pets","wire":"owners:<owner_id>:pets","counts":{)

      assert json =~
               ~s("counts":{"broadcast":2,"handle":1,"ignore":0,"subscribe":1},"subscribes":[)

      assert json =~
               ~s({"name":"adopted","counts":{"broadcast":2,"handle":1,"ignore":0},"call_sites":[)

      assert json =~
               ~s({"kind":"broadcast","file":"lib/my_app/pets.ex","line":12,"module":"MyApp.Pets"})
    end

    test "writes the keys of the summary in the same order" do
      assert {:ok, json} = Json.render(report(:summary))

      assert json =~ ~s({"name":"pets","wire":"owners:<owner_id>:pets","counts":{)

      assert json =~
               ~s(,"events":[{"name":"adopted","counts":{"broadcast":2,"handle":1,"ignore":0}}])
    end

    test "writes a module without the Elixir prefix and a name without the colon" do
      assert {:ok, json} = Json.render(report(:detail))
      refute json =~ "Elixir."

      assert %{"manifests" => [%{"module" => "MyApp.Topics", "topics" => [topic]}]} =
               JSON.decode!(json)

      assert topic["name"] == "pets"
      assert [%{"name" => "adopted"}] = topic["events"]
    end
  end

  defp decode(report) do
    {:ok, json} = Json.render(report)
    JSON.decode!(json)
  end

  defp report(view) do
    topic = %Topic{
      name: :pets,
      wire: "owners:<owner_id>:pets",
      subscribes: [entry(:subscribe, "lib/my_app/notifier.ex", 22, MyApp.Notifier)],
      events: [
        %Event{
          name: :adopted,
          entries: [
            entry(:broadcast, "lib/my_app/pets.ex", 12, MyApp.Pets),
            entry(:broadcast_from, "lib/my_app_web/live/pets_live.ex", 51, MyAppWeb.PetsLive),
            entry(:handle, "lib/my_app_web/live/pets_live.ex", 30, MyAppWeb.PetsLive)
          ]
        }
      ]
    }

    %Report{view: view, manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]}
  end

  defp entry(kind, file, line, module) do
    %Entry{
      manifest: MyApp.Topics,
      topic: :pets,
      event: :adopted,
      kind: kind,
      module: module,
      file: file,
      line: line
    }
  end
end

defmodule VerifiedPubSub.Usage.JsonVersionTest do
  # This module has no :json tag, so it runs on each version of Elixir.
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Usage.Json

  describe "supported?/1" do
    test "is true from Elixir 1.18, which has the JSON module" do
      assert Json.supported?("1.18.0")
      assert Json.supported?("1.18.0-rc.0")
      assert Json.supported?("1.20.4")
    end

    test "is false before Elixir 1.18, where a JSON module can only come from a dependency" do
      refute Json.supported?("1.17.3")
      refute Json.supported?("1.16.0")
    end
  end
end
