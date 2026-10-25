defmodule VerifiedPubSub.PubSubDocsTest do
  # Not async, because the `:docs` compiler option is global and the test env turns it off.
  use ExUnit.Case, async: false

  import VerifiedPubSub.CompileHelper

  @moduletag :tmp_dir

  describe "use VerifiedPubSub with docs on" do
    test "lists each published topic once, with its events", %{tmp_dir: tmp_dir} do
      doc =
        moduledoc!(tmp_dir, """
        @moduledoc "Manages pets."
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        def adopt(id), do: broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "p1"})
        def adopt_again(id), do: broadcast(:pets, %{owner_id: id}, :adopted, %{id: "p2"})
        def rehome(id), do: notify(id)
        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        defp notify(id), do: broadcast_from(self(), :pets, %{owner_id: id}, :rehomed, %{id: "p1"})
        """)

      assert doc == """
             Manages pets.

             ## PubSub

               * Publishes to `"owners:<owner_id>:pets"` for `:adopted` and `:rehomed`.
               * Publishes to `"system"` for `:alert`.
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

      assert doc == "Alerts.\n\n## PubSub\n\n  * Publishes to `\"system\"` for `:alert`.\n"
    end

    test "writes the section as the whole moduledoc when the module has none", %{
      tmp_dir: tmp_dir
    } do
      doc =
        moduledoc!(tmp_dir, """
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

        def alert, do: broadcast!(:system, :alert, %{text: "hi"})
        """)

      assert doc == "## PubSub\n\n  * Publishes to `\"system\"` for `:alert`.\n"
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
      assert doc =~ ~s(  * Publishes to `"bulk"` for `:"bulk-update"`.)
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

  defp moduledoc!(tmp_dir, body) do
    %{"en" => doc} = fetch_moduledoc!(tmp_dir, module_source(body))
    String.trim_trailing(doc) <> "\n"
  end

  defp module_source(body) do
    "defmodule #{unique_module("VerifiedPubSub.PubSubDocsTest.Module")} do\n#{body}\nend"
  end
end
