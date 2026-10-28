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

  describe "handle_message/5" do
    test "records one :handle entry for each event in the list" do
      {module, source} =
        compile_subscriber("""
        handle_message :pets, [:adopted, :renamed], _payload, state do
          {:noreply, state}
        end

        ignore_message :pets, :rehomed
        """)

      handles = Enum.filter(Usage.entries(module), &(&1.kind == :handle))

      assert handles == [
               entry(module, source, "handle_message", :pets, :adopted, :handle),
               entry(module, source, "handle_message", :pets, :renamed, :handle)
             ]
    end
  end

  describe "handle_message/5 with a broadcast in the body" do
    test "records the broadcast with the line of the body" do
      {module, source} =
        compile_subscriber("""
        handle_message :pets, [:adopted, :renamed, :rehomed], _payload, state do
          broadcast!(:system, :alert, %{text: "x"})
          {:noreply, state}
        end
        """)

      broadcasts = Enum.filter(Usage.entries(module), &(&1.kind == :broadcast))

      assert broadcasts == [entry(module, source, "broadcast!", :system, :alert, :broadcast)]
    end
  end

  describe "ignore_message/2" do
    test "records one :ignore entry for each event in the list" do
      {module, source} =
        compile_subscriber("""
        ignore_message :pets, [:adopted, :renamed]
        ignore_message :pets, :rehomed
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

        handle_message :pets, [:adopted, :renamed, :rehomed], _payload, state do
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
      helper = unique_module("VPTest.UsageHelper")

      compile!("""
      defmodule #{helper} do
        defmacro __using__(_opts) do
          quote do
            use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
            def alert, do: broadcast!(:system, :alert, %{text: "x"})
          end
        end
      end
      """)

      [first, second] =
        Enum.map(1..2, fn _index ->
          "defmodule #{unique_module("VPTest.Usage")} do\n  use #{helper}\nend\n"
          |> compile_file!()
          |> Usage.entries()
        end)

      assert [%Entry{kind: :broadcast, topic: :system, event: :alert} = one] = first
      assert [%Entry{kind: :broadcast, topic: :system, event: :alert} = two] = second
      assert {one.file, one.line} == {two.file, two.line}
      assert one.module != two.module
    end
  end

  describe "VerifiedPubSub.Usage.entries/1" do
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
