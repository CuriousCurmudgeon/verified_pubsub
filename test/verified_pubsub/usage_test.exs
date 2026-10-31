defmodule VerifiedPubSub.UsageTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.TestManifests.Basic
  alias VerifiedPubSub.Usage
  alias VerifiedPubSub.Usage.Entry

  @file_name "lib/usage_fixture.ex"

  describe "broadcast/4, broadcast!/4, broadcast/3, broadcast!/3" do
    test "records a :broadcast entry with the topic, the event, the module, the file, and the line" do
      {module, source} =
        compile_body("""
        def a, do: broadcast(:pets, %{owner_id: "1"}, :adopted, %{id: "1"})
        def b, do: broadcast!(:pets, %{owner_id: "1"}, :adopted, %{id: "1"})
        def c, do: broadcast(:system, :alert, %{text: "x"})
        def d, do: broadcast!(:system, :alert, %{text: "x"})
        """)

      assert Usage.entries(module) == [
               entry(module, source, "def a", :pets, :adopted, :broadcast),
               entry(module, source, "def b", :pets, :adopted, :broadcast),
               entry(module, source, "def c", :system, :alert, :broadcast),
               entry(module, source, "def d", :system, :alert, :broadcast)
             ]
    end
  end

  describe "broadcast_from/5, broadcast_from!/5, broadcast_from/4, broadcast_from!/4" do
    test "records a :broadcast_from entry" do
      {module, source} =
        compile_body("""
        def a, do: broadcast_from(self(), :pets, %{owner_id: "1"}, :adopted, %{id: "1"})
        def b, do: broadcast_from!(self(), :pets, %{owner_id: "1"}, :adopted, %{id: "1"})
        def c, do: broadcast_from(self(), :system, :alert, %{text: "x"})
        def d, do: broadcast_from!(self(), :system, :alert, %{text: "x"})
        """)

      assert Usage.entries(module) == [
               entry(module, source, "def a", :pets, :adopted, :broadcast_from),
               entry(module, source, "def b", :pets, :adopted, :broadcast_from),
               entry(module, source, "def c", :system, :alert, :broadcast_from),
               entry(module, source, "def d", :system, :alert, :broadcast_from)
             ]
    end
  end

  describe "subscribe/2, subscribe/1" do
    test "records a :subscribe entry with no event" do
      {module, source} =
        compile_body("""
        def a, do: subscribe(:pets, %{owner_id: "1"})
        def b, do: subscribe(:system)
        """)

      assert Usage.entries(module) == [
               entry(module, source, "def a", :pets, nil, :subscribe),
               entry(module, source, "def b", :system, nil, :subscribe)
             ]
    end
  end

  describe "unsubscribe/2, unsubscribe/1" do
    test "records an :unsubscribe entry with no event" do
      {module, source} =
        compile_body("""
        def a, do: unsubscribe(:pets, %{owner_id: "1"})
        def b, do: unsubscribe(:system)
        """)

      assert Usage.entries(module) == [
               entry(module, source, "def a", :pets, nil, :unsubscribe),
               entry(module, source, "def b", :system, nil, :unsubscribe)
             ]
    end
  end

  describe "topic/2" do
    test "records no entry" do
      {module, _source} = compile_body(~s|def a, do: topic(:pets, %{owner_id: "1"})|)

      assert Usage.entries(module) == []
    end
  end

  describe "handle_broadcast/5" do
    test "records one :handle entry for each event in the list" do
      {module, source} =
        compile_subscriber("""
        handle_broadcast :pets, [:adopted, :renamed], _payload, state do
          {:noreply, state}
        end

        ignore_broadcast :pets, :rehomed
        """)

      handles = Enum.filter(Usage.entries(module), &(&1.kind == :handle))

      assert handles == [
               entry(module, source, "handle_broadcast", :pets, :adopted, :handle),
               entry(module, source, "handle_broadcast", :pets, :renamed, :handle)
             ]
    end
  end

  describe "handle_broadcast/5 with a broadcast in the body" do
    test "records the broadcast with the line of the body" do
      {module, source} =
        compile_subscriber("""
        handle_broadcast :pets, [:adopted, :renamed, :rehomed], _payload, state do
          broadcast!(:system, :alert, %{text: "x"})
          {:noreply, state}
        end
        """)

      broadcasts = Enum.filter(Usage.entries(module), &(&1.kind == :broadcast))

      assert broadcasts == [entry(module, source, "broadcast!", :system, :alert, :broadcast)]
    end
  end

  describe "ignore_broadcast/2" do
    test "records one :ignore entry for each event in the list" do
      {module, source} =
        compile_subscriber("""
        ignore_broadcast :pets, [:adopted, :renamed]
        ignore_broadcast :pets, :rehomed
        """)

      assert Usage.entries(module) == [
               entry(module, source, "[:adopted", :pets, :adopted, :ignore),
               entry(module, source, "[:adopted", :pets, :renamed, :ignore),
               entry(module, source, ":rehomed", :pets, :rehomed, :ignore)
             ]
    end
  end

  describe "use VerifiedPubSub, use VerifiedPubSub.Subscriber" do
    test "records each call once in a module with both use lines" do
      source = """
      defmodule #{unique_module("VPTest.Usage")} do
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
        use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

        def a, do: broadcast!(:system, :alert, %{text: "x"})

        handle_broadcast :pets, [:adopted, :renamed, :rehomed], _payload, state do
          {:noreply, state}
        end
      end
      """

      module = compile_file!(source)

      assert module
             |> Usage.entries()
             |> Enum.map(&{&1.kind, &1.event})
             |> Enum.sort() == [
               {:broadcast, :alert},
               {:handle, :adopted},
               {:handle, :rehomed},
               {:handle, :renamed}
             ]
    end
  end

  describe "a broadcast in a shared __using__" do
    test "records one entry for each module that uses it" do
      helper = compile_helper(:quote)

      [one, two] = Enum.map(["lib/one.ex", "lib/two.ex"], &use_helper(helper, &1, 2))

      assert [%Entry{kind: :broadcast, topic: :system, event: :alert}] = one
      assert [%Entry{kind: :broadcast, topic: :system, event: :alert}] = two
      assert hd(one).module != hd(two).module
    end

    test "records the use line of each module" do
      helper = compile_helper(:quote)

      assert [%Entry{file: "lib/one.ex", line: 2}] = use_helper(helper, "lib/one.ex", 2)
      assert [%Entry{file: "lib/two.ex", line: 4}] = use_helper(helper, "lib/two.ex", 4)
    end

    test "records the line in the helper when the quote keeps its location" do
      helper = compile_helper(:keep)

      # Elixir gives a kept location an absolute path.
      for file <- ["lib/one.ex", "lib/two.ex"] do
        assert [%Entry{file: path, line: 5}] = use_helper(helper, file, 2)
        assert Path.relative_to_cwd(path) == "lib/helper.ex"
      end
    end
  end

  describe "entries/1" do
    test "returns an empty list for a module that binds no manifest" do
      assert Usage.entries(Enum) == []
    end

    test "returns an empty list for a module that does not exist" do
      assert Usage.entries(VPTest.UsageDeleted) == []
    end

    test "returns an empty list for an Erlang module" do
      assert Usage.entries(:lists) == []
    end
  end

  defp compile_body(body) do
    source = """
    defmodule #{unique_module("VPTest.Usage")} do
      use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

    #{body}
    end
    """

    {compile_file!(source), source}
  end

  defp compile_subscriber(body) do
    source = """
    defmodule #{unique_module("VPTest.Usage")} do
      use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

    #{body}
    end
    """

    {compile_file!(source), source}
  end

  # The broadcast is on line 5 of the helper.
  defp compile_helper(location) do
    quote_line = if location == :keep, do: "quote location: :keep do", else: "quote do"
    helper = unique_module("VPTest.UsageHelper")

    Code.compile_string(
      """
      defmodule #{helper} do
        defmacro __using__(_opts) do
          #{quote_line}
            use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
            def alert, do: broadcast!(:system, :alert, %{text: "x"})
          end
        end
      end
      """,
      "lib/helper.ex"
    )

    helper
  end

  # Compiles a module in `file` that uses `helper` on line `line`.
  defp use_helper(helper, file, line) do
    blank_lines = String.duplicate("\n", line - 2)
    source = "defmodule #{unique_module("VPTest.Usage")} do\n#{blank_lines}  use #{helper}\nend\n"
    [{module, _bin} | _] = Code.compile_string(source, file)

    Usage.entries(module)
  end

  defp compile_file!(source) do
    [{module, _bin} | _] = Code.compile_string(source, @file_name)
    module
  end

  # The entry of the first line of `source` that holds `needle`.
  defp entry(module, source, needle, topic, event, kind) do
    line =
      source
      |> String.split("\n")
      |> Enum.find_index(&String.contains?(&1, needle))

    %Entry{
      manifest: Basic,
      topic: topic,
      event: event,
      kind: kind,
      module: module,
      file: @file_name,
      line: line + 1
    }
  end
end
