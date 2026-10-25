defmodule VerifiedPubSub.PubSubDocsTest do
  # Not async, because the `:docs` compiler option is global and the test env turns it off.
  use ExUnit.Case, async: false

  import VerifiedPubSub.CompileHelper

  @moduletag :tmp_dir

  @alert_section """
  ## PubSub

  | Topic | Publishes | Subscribes |
  | --- | --- | --- |
  | `:system` (`"system"`) | `:alert` | |
  """

  describe "use VerifiedPubSub with docs on" do
    test "lists each published topic once, in the order of the manifest", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        @moduledoc "Manages pets."
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        def bare, do: broadcast!(:typed_payloads, %{x: "1"}, :bare, %{id: "b1"})
        def rehome(id), do: notify(id)
        def adopt(id), do: broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "p1"})
        def adopt_again(id), do: broadcast(:pets, %{owner_id: id}, :adopted, %{id: "p2"})
        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        defp notify(id), do: broadcast_from(self(), :pets, %{owner_id: id}, :rehomed, %{id: "p1"})
        """)

      assert doc == """
             Manages pets.

             ## PubSub

             | Topic | Publishes | Subscribes |
             | --- | --- | --- |
             | `:pets` (`"owners:<owner_id>:pets"`) | `:adopted`, `:rehomed` | |
             | `:system` (`"system"`) | `:alert` | |
             | `:typed_payloads` (`"payloads:<x>"`) | `:bare` | |
             """
    end

    test "adds the section after a moduledoc below the broadcasts", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        def alert, do: broadcast!(:system, :alert, %{text: "hi"})

        @moduledoc \"\"\"
        Alerts.
        \"\"\"
        """)

      assert doc == "Alerts.\n\n" <> @alert_section
    end

    test "writes the section as the whole moduledoc when the module has none", %{
      tmp_dir: tmp_dir
    } do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        """)

      assert doc == @alert_section
    end

    test "keeps the line of defmodule when the module has no moduledoc", %{tmp_dir: tmp_dir} do
      source =
        "\n\n" <>
          module_source("""
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

          def alert, do: broadcast!(:system, :alert, %{text: "hi"})
          """)

      assert {:docs_v1, anno, _, _, %{"en" => _}, _, _} = fetch_docs!(tmp_dir, source)
      assert :erl_anno.line(anno) == 3
    end

    test "keeps a module with @moduledoc false hidden", %{tmp_dir: tmp_dir} do
      assert :hidden ==
               fetch_moduledoc!(
                 tmp_dir,
                 module_source("""
                 @moduledoc false
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

                 def alert, do: broadcast!(:system, :alert, %{text: "hi"})
                 """)
               )
    end

    test "adds no section to a module with no broadcast", %{tmp_dir: tmp_dir} do
      assert :none ==
               fetch_moduledoc!(
                 tmp_dir,
                 module_source("""
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true
                 """)
               )
    end

    test "quotes an event name that needs quotes", %{tmp_dir: tmp_dir} do
      manifest = unique_module("VerifiedPubSub.PubSubDocsTest.Manifest")

      source = """
      defmodule #{manifest} do
        use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

        topic :bulk, ["bulk"] do
          message :"bulk-update"
        end
      end

      #{module_source("""
      use VerifiedPubSub, manifest: #{manifest}, docs: true

      def update, do: broadcast!(:bulk, :"bulk-update", %{})
      """)}
      """

      assert %{"en" => doc} = fetch_moduledoc!(tmp_dir, source)
      assert doc =~ ~s[| `:bulk` (`"bulk"`) | `:"bulk-update"` | |]
    end
  end

  describe "use VerifiedPubSub with an invalid docs value" do
    test "is a compile error that names the value" do
      error =
        compile_error(
          module_source("""
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: :yes
          """)
        )

      assert %ArgumentError{message: "expected a boolean for :docs, got: :yes"} = error
    end
  end

  describe "use VerifiedPubSub.Manifest with docs on" do
    test "turns on the section for each module that binds it", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Documented

        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        """)

      assert doc == @alert_section
    end

    test "lets a use line turn the section off", %{tmp_dir: tmp_dir} do
      assert :none ==
               fetch_moduledoc!(
                 tmp_dir,
                 module_source("""
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Documented, docs: false

                 def alert, do: broadcast!(:system, :alert, %{text: "hi"})
                 """)
               )
    end
  end

  describe "use VerifiedPubSub with docs off" do
    test "adds no section by default", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        @moduledoc "Pets."
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        """)

      assert doc == "Pets.\n"
    end
  end

  describe "use VerifiedPubSub with docs on and the :docs compiler option off" do
    test "compiles, and its broadcasts still deliver" do
      module =
        compile!(
          module_source("""
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

          def alert, do: broadcast!(:system, :alert, %{text: "hi"})
          """)
        )

      :ok = Phoenix.PubSub.subscribe(VerifiedPubSub.TestPubSub, "system")
      module.alert()

      assert_receive %VerifiedPubSub.Message{event: :alert}
    end
  end

  describe "use VerifiedPubSub.Subscriber with docs on" do
    test "lists the handled events, and leaves out the ignored events", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        handle_message :pets, [:rehomed, :adopted], payload, state do
          broadcast!(:system, :alert, %{text: payload.id})
          {:noreply, state}
        end

        handle_message :pets, :renamed, _payload, state do
          {:noreply, state}
        end

        ignore_message :system, :alert
        """)

      assert doc == """
             ## PubSub

             | Topic | Publishes | Subscribes |
             | --- | --- | --- |
             | `:pets` (`"owners:<owner_id>:pets"`) | | `:adopted`, `:renamed`, `:rehomed` |
             | `:system` (`"system"`) | `:alert` | |
             """
    end

    test "lists no topic for a subscribe call", %{tmp_dir: tmp_dir} do
      assert :none ==
               fetch_moduledoc!(
                 tmp_dir,
                 module_source("""
                 use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

                 def join, do: subscribe(:system)

                 ignore_message :pets, [:adopted, :renamed, :rehomed]
                 ignore_message :system, :alert
                 """)
               )
    end
  end

  describe "use VerifiedPubSub with docs on and a subscribe call" do
    test "adds no row for the subscribed topic", %{tmp_dir: tmp_dir} do
      assert :none ==
               fetch_moduledoc!(
                 tmp_dir,
                 module_source("""
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

                 def join, do: subscribe(:system)
                 def join(id), do: subscribe(:pets, %{owner_id: id})
                 """)
               )
    end
  end

  describe "a module with both use lines" do
    @both_expected """
    ## PubSub

    | Topic | Publishes | Subscribes |
    | --- | --- | --- |
    | `:system` (`"system"`) | `:alert` | `:alert` |
    """

    @handlers """
    handle_message :system, :alert, payload, state do
      broadcast!(:system, :alert, payload)
      {:noreply, state}
    end

    ignore_message :pets, [:adopted, :renamed, :rehomed]
    """

    test "gets one section when use VerifiedPubSub comes first", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true
        use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

        #{@handlers}
        """)

      assert doc == @both_expected
    end

    test "gets one section when use VerifiedPubSub.Subscriber comes first", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic, docs: true
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

        #{@handlers}
        """)

      assert doc == @both_expected
    end

    test "is a compile error when the lines give different docs values" do
      error =
        compile_error(
          module_source("""
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true
          use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic, docs: false
          """)
        )

      assert %ArgumentError{message: message} = error
      assert message =~ "docs: true"
      assert message =~ "docs: false"
    end

    test "gets one section when use VerifiedPubSub appears twice", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        """)

      assert doc == @alert_section
    end
  end

  defp moduledoc!(tmp_dir, body) do
    %{"en" => doc} = fetch_moduledoc!(tmp_dir, module_source(body))
    String.trim_trailing(doc) <> "\n"
  end

  defp module_source(body) do
    "defmodule #{unique_module("VerifiedPubSub.PubSubDocsTest.Module")} do\n#{body}\nend"
  end
end
