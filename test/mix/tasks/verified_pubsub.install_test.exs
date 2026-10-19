defmodule Mix.Tasks.VerifiedPubsub.InstallTest do
  use ExUnit.Case, async: true

  import Igniter.Test

  @mix_exs """
  defmodule Test.MixProject do
    use Mix.Project

    def project do
      [app: :test, version: "0.1.0", deps: []]
    end

    def application do
      [mod: {Test.Application, []}, extra_applications: [:logger]]
    end
  end
  """

  defp project_with_children(children) do
    test_project(
      files: %{
        "mix.exs" => @mix_exs,
        "lib/test/application.ex" => """
        defmodule Test.Application do
          use Application

          @impl true
          def start(_type, _args) do
            children = #{children}

            Supervisor.start_link(children, strategy: :one_for_one, name: Test.Supervisor)
          end
        end
        """
      }
    )
  end

  defp install(igniter, argv \\ []) do
    Igniter.compose_task(igniter, "verified_pubsub.install", argv)
  end

  describe "mix verified_pubsub.install" do
    test "imports the formatter rules" do
      project_with_children("[]")
      |> install()
      |> assert_has_patch(".formatter.exs", """
      + |  import_deps: [:verified_pubsub]
      """)
    end

    test "uses the PubSub the application already starts" do
      project_with_children("[{Phoenix.PubSub, name: Test.Events}]")
      |> install()
      |> assert_unchanged("lib/test/application.ex")
      |> assert_creates("lib/test/topics.ex", """
      defmodule Test.Topics do
        use VerifiedPubSub.Manifest, pubsub: Test.Events
      end
      """)
    end

    test "starts a PubSub when the application has none" do
      project_with_children("[]")
      |> install()
      |> assert_has_patch("lib/test/application.ex", """
      + |    children = [{Phoenix.PubSub, [name: Test.PubSub]}]
      """)
      |> assert_creates("lib/test/topics.ex", """
      defmodule Test.Topics do
        use VerifiedPubSub.Manifest, pubsub: Test.PubSub
      end
      """)
    end

    test "--pubsub and --manifest override the defaults" do
      project_with_children("[]")
      |> install(["--pubsub", "Test.Bus", "--manifest", "Test.Events.Manifest"])
      |> assert_has_patch("lib/test/application.ex", """
      + |    children = [{Phoenix.PubSub, [name: Test.Bus]}]
      """)
      |> assert_creates("lib/test/events/manifest.ex", """
      defmodule Test.Events.Manifest do
        use VerifiedPubSub.Manifest, pubsub: Test.Bus
      end
      """)
    end

    test "does not start a second PubSub when --pubsub names a different one" do
      project_with_children("[{Phoenix.PubSub, name: Test.PubSub}]")
      |> install(["--pubsub", "Test.Bus"])
      |> assert_unchanged("lib/test/application.ex")
      |> assert_has_warning(&(&1 =~ "Test.Bus\nwas not added"))
      |> assert_creates("lib/test/topics.ex", """
      defmodule Test.Topics do
        use VerifiedPubSub.Manifest, pubsub: Test.Bus
      end
      """)
    end

    test "leaves an existing manifest alone" do
      igniter =
        project_with_children("[{Phoenix.PubSub, name: Test.PubSub}]")
        |> Igniter.create_new_file("lib/test/topics.ex", """
        defmodule Test.Topics do
          use VerifiedPubSub.Manifest, pubsub: Test.PubSub

          topic :pets, ["pets"] do
            message :adopted
          end
        end
        """)
        |> apply_igniter!()

      igniter
      |> install()
      |> assert_unchanged("lib/test/topics.ex")
    end

    test "warns when the existing PubSub name cannot be read" do
      project_with_children("[{Phoenix.PubSub, name: pubsub_name()}]")
      |> install()
      |> assert_has_warning(&(&1 =~ "could not\nread the name"))
    end
  end
end
