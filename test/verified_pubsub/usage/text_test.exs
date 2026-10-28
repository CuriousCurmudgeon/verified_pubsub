defmodule VerifiedPubSub.Usage.TextTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Usage.{Entry, Report, Text}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  describe "render/1" do
    test "renders the overview as one table for each manifest" do
      report = %Report{
        view: :overview,
        manifests: [
          %Manifest{module: MyApp.Topics, topics: [pets(), treats()]},
          %Manifest{
            module: Other.Topics,
            topics: [
              %Topic{
                name: :system,
                wire: "system",
                subscribes: [],
                events: [%Event{name: :alert, entries: []}]
              }
            ]
          }
        ]
      }

      assert Text.render(report) <> "\n" == """
             MyApp.Topics

             Topic    Wire topic                Events  Broadcasts  Handlers  Subscribes
             :pets    owners:<owner_id>:pets         2           3         3           2
             :treats  owners:<owner_id>:treats       1           1         0           1

             Other.Topics

             Topic    Wire topic  Events  Broadcasts  Handlers  Subscribes
             :system  system           1           0         0           0
             """
    end

    test "renders a line that says the project has no manifest" do
      assert Text.render(%Report{view: :overview, manifests: []}) ==
               "No VerifiedPubSub manifest is in this project."
    end

    test "renders a topic with its subscribes and each event" do
      report = %Report{
        view: :topic,
        manifests: [%Manifest{module: MyApp.Topics, topics: [pets()]}]
      }

      assert Text.render(report) <> "\n" == """
             :pets  owners:<owner_id>:pets  (MyApp.Topics)

             Subscribes (2)
               subscribe        lib/my_app_web/live/pets_live.ex:14  MyAppWeb.PetsLive
               subscribe        lib/my_app/notifier.ex:22            MyApp.Notifier

             :adopted  3 broadcasts, 2 handlers
               broadcast        lib/my_app/pets.ex:12                MyApp.Pets
               broadcast        lib/my_app/imports.ex:40             MyApp.Imports
               broadcast_from   lib/my_app_web/live/pets_live.ex:51  MyAppWeb.PetsLive
               handle_message   lib/my_app_web/live/pets_live.ex:30  MyAppWeb.PetsLive
               handle_message   lib/my_app/notifier.ex:30            MyApp.Notifier

             :rehomed  0 broadcasts, 1 handler, 1 ignore  (no broadcast)
               handle_message   lib/my_app/notifier.ex:34            MyApp.Notifier
               ignore_message   lib/my_app_web/live/pets_live.ex:40  MyAppWeb.PetsLive
             """
    end

    test "renders one event with the subscribes of its topic" do
      topic = treats()

      report = %Report{
        view: :event,
        manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]
      }

      assert Text.render(report) <> "\n" == """
             :treats  owners:<owner_id>:treats  (MyApp.Topics)

             Subscribes (1)
               subscribe        lib/my_app/notifier.ex:50  MyApp.Notifier

             :given  1 broadcast
               broadcast        lib/my_app/treats.ex:8     MyApp.Treats
             """
    end

    test "flags an event with no broadcast" do
      topic = %Topic{
        name: :system,
        wire: "system",
        subscribes: [],
        events: [%Event{name: :alert, entries: []}]
      }

      report = %Report{
        view: :topic,
        manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]
      }

      assert Text.render(report) <> "\n" == """
             :system  system  (MyApp.Topics)

             Subscribes (0)

             :alert  0 broadcasts  (no broadcast)
             """
    end

    test "uses the singular for a count of one" do
      topic = %Topic{
        name: :system,
        wire: "system",
        subscribes: [],
        events: [
          %Event{
            name: :alert,
            entries: [
              entry(:broadcast, "lib/a.ex", 1, A),
              entry(:handle, "lib/b.ex", 2, B),
              entry(:ignore, "lib/c.ex", 3, C)
            ]
          }
        ]
      }

      report = %Report{
        view: :topic,
        manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]
      }

      assert Text.render(report) =~ ":alert  1 broadcast, 1 handler, 1 ignore\n"
    end
  end

  defp pets do
    %Topic{
      name: :pets,
      wire: "owners:<owner_id>:pets",
      subscribes: [
        entry(:subscribe, "lib/my_app_web/live/pets_live.ex", 14, MyAppWeb.PetsLive),
        entry(:subscribe, "lib/my_app/notifier.ex", 22, MyApp.Notifier)
      ],
      events: [
        %Event{
          name: :adopted,
          entries: [
            entry(:broadcast, "lib/my_app/pets.ex", 12, MyApp.Pets),
            entry(:broadcast, "lib/my_app/imports.ex", 40, MyApp.Imports),
            entry(:broadcast_from, "lib/my_app_web/live/pets_live.ex", 51, MyAppWeb.PetsLive),
            entry(:handle, "lib/my_app_web/live/pets_live.ex", 30, MyAppWeb.PetsLive),
            entry(:handle, "lib/my_app/notifier.ex", 30, MyApp.Notifier)
          ]
        },
        %Event{
          name: :rehomed,
          entries: [
            entry(:handle, "lib/my_app/notifier.ex", 34, MyApp.Notifier),
            entry(:ignore, "lib/my_app_web/live/pets_live.ex", 40, MyAppWeb.PetsLive)
          ]
        }
      ]
    }
  end

  defp treats do
    %Topic{
      name: :treats,
      wire: "owners:<owner_id>:treats",
      subscribes: [entry(:subscribe, "lib/my_app/notifier.ex", 50, MyApp.Notifier)],
      events: [
        %Event{
          name: :given,
          entries: [entry(:broadcast, "lib/my_app/treats.ex", 8, MyApp.Treats)]
        }
      ]
    }
  end

  defp entry(kind, file, line, module) do
    %Entry{
      manifest: MyApp.Topics,
      topic: :pets,
      event: nil,
      kind: kind,
      module: module,
      file: file,
      line: line
    }
  end
end
