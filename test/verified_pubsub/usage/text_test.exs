defmodule VerifiedPubSub.Usage.TextTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Usage.{Entry, Report, Text}
  alias VerifiedPubSub.Usage.Report.{Event, Manifest, Topic}

  describe "render/1" do
    test "renders the summary as one table for each manifest" do
      report = %Report{
        view: :summary,
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

      assert plain(report) <> "\n" == """
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
      for view <- [:summary, :detail] do
        assert plain(%Report{view: view, manifests: []}) ==
                 "No VerifiedPubSub manifest is in this project."
      end
    end

    test "groups the topics of the detail view under the name of each manifest" do
      report = %Report{
        view: :detail,
        manifests: [
          %Manifest{module: MyApp.Topics, topics: [treats(), system()]},
          %Manifest{module: Other.Topics, topics: [treats()]}
        ]
      }

      assert plain(report) <> "\n" == """
             MyApp.Topics

             :treats  owners:<owner_id>:treats

             Subscribes (1)
               subscribe          lib/my_app/notifier.ex:50  MyApp.Notifier

             :given  1 broadcast
               broadcast          lib/my_app/treats.ex:8     MyApp.Treats

             :system  system

             Subscribes: none

             :alert  0 broadcasts  (no broadcast)

             Other.Topics

             :treats  owners:<owner_id>:treats

             Subscribes (1)
               subscribe          lib/my_app/notifier.ex:50  MyApp.Notifier

             :given  1 broadcast
               broadcast          lib/my_app/treats.ex:8     MyApp.Treats
             """
    end

    test "pads the locations of each topic to the longest location of that topic" do
      report = %Report{
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [pets(), treats()]}]
      }

      output = plain(report)

      assert output =~ "\n  broadcast          lib/my_app/pets.ex:12                MyApp.Pets\n"
      assert output =~ "\n  broadcast          lib/my_app/treats.ex:8     MyApp.Treats"
    end

    test "renders one line for a topic with no subscribe" do
      report = %Report{
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [system()]}]
      }

      assert plain(report) =~ "\n:system  system\n\nSubscribes: none\n\n:alert"
    end

    test "renders a topic with its subscribes and each event" do
      report = %Report{
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [pets()]}]
      }

      assert plain(report) <> "\n" == """
             MyApp.Topics

             :pets  owners:<owner_id>:pets

             Subscribes (2)
               subscribe          lib/my_app_web/live/pets_live.ex:14  MyAppWeb.PetsLive
               subscribe          lib/my_app/notifier.ex:22            MyApp.Notifier

             :adopted  3 broadcasts, 2 handlers
               broadcast          lib/my_app/pets.ex:12                MyApp.Pets
               broadcast          lib/my_app/imports.ex:40             MyApp.Imports
               broadcast_from     lib/my_app_web/live/pets_live.ex:51  MyAppWeb.PetsLive
               handle_broadcast   lib/my_app_web/live/pets_live.ex:30  MyAppWeb.PetsLive
               handle_broadcast   lib/my_app/notifier.ex:30            MyApp.Notifier

             :rehomed  0 broadcasts, 1 handler, 1 ignore  (no broadcast)
               handle_broadcast   lib/my_app/notifier.ex:34            MyApp.Notifier
               ignore_broadcast   lib/my_app_web/live/pets_live.ex:40  MyAppWeb.PetsLive
             """
    end

    test "renders one event with the subscribes of its topic" do
      topic = treats()

      report = %Report{
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]
      }

      assert plain(report) <> "\n" == """
             MyApp.Topics

             :treats  owners:<owner_id>:treats

             Subscribes (1)
               subscribe          lib/my_app/notifier.ex:50  MyApp.Notifier

             :given  1 broadcast
               broadcast          lib/my_app/treats.ex:8     MyApp.Treats
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
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]
      }

      assert plain(report) <> "\n" == """
             MyApp.Topics

             :system  system

             Subscribes: none

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
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [topic]}]
      }

      assert plain(report) =~ ":alert  1 broadcast, 1 handler, 1 ignore\n"
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

  describe "render/1 with ANSI" do
    setup do
      report = %Report{
        view: :detail,
        manifests: [%Manifest{module: MyApp.Topics, topics: [pets(), system()]}]
      }

      %{ansi: ansi(report)}
    end

    test "colors the name of a topic cyan and bolds the name of the manifest and of an event", %{
      ansi: ansi
    } do
      assert ansi =~ IO.ANSI.bright() <> "MyApp.Topics" <> IO.ANSI.reset()
      assert ansi =~ IO.ANSI.cyan() <> ":pets" <> IO.ANSI.reset()
      assert ansi =~ IO.ANSI.bright() <> ":adopted" <> IO.ANSI.reset()
    end

    test "colors a send green, a receive magenta, and an ignore faint", %{ansi: ansi} do
      assert ansi =~ IO.ANSI.green() <> "broadcast      "
      assert ansi =~ IO.ANSI.green() <> "broadcast_from "
      assert ansi =~ IO.ANSI.magenta() <> "handle_broadcast "
      assert ansi =~ IO.ANSI.magenta() <> "subscribe      "
      assert ansi =~ IO.ANSI.faint() <> "ignore_broadcast "
    end

    test "colors the flag of an event with no broadcast yellow", %{ansi: ansi} do
      assert ansi =~ IO.ANSI.yellow() <> "(no broadcast)" <> IO.ANSI.reset()
    end

    test "bolds the header row of the summary and colors each topic cyan" do
      report = %Report{
        view: :summary,
        manifests: [%Manifest{module: MyApp.Topics, topics: [pets()]}]
      }

      ansi = ansi(report)

      assert ansi =~ IO.ANSI.bright() <> "Topic  "
      assert ansi =~ IO.ANSI.cyan() <> ":pets"
    end

    test "keeps the columns of the summary aligned" do
      report = %Report{
        view: :summary,
        manifests: [%Manifest{module: MyApp.Topics, topics: [pets(), treats()]}]
      }

      assert report |> ansi() |> strip_ansi() == plain(report)
    end
  end

  defp plain(report) do
    report
    |> Text.render()
    |> IO.ANSI.format(false)
    |> IO.iodata_to_binary()
  end

  defp ansi(report) do
    report
    |> Text.render()
    |> IO.ANSI.format(true)
    |> IO.iodata_to_binary()
  end

  defp strip_ansi(text), do: String.replace(text, ~r/\e\[[0-9;]*m/, "")

  defp system do
    %Topic{
      name: :system,
      wire: "system",
      subscribes: [],
      events: [%Event{name: :alert, entries: []}]
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
