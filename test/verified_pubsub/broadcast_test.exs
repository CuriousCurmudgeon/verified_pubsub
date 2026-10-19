defmodule VerifiedPubSub.BroadcastTest do
  use ExUnit.Case, async: true
  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestManifests.Basic

  import VerifiedPubSub.CompileHelper

  setup do
    %{owner_id: unique_owner_id()}
  end

  test "topic/2 interpolates params into the wire pattern" do
    assert topic(:pets, %{owner_id: "7"}) == "owners:7:pets"
  end

  test "topic/2 stringifies non-binary params" do
    assert topic(:pets, %{owner_id: 7}) == "owners:7:pets"
  end

  test "Info.match/2 recovers the name and params from what topic/2 builds" do
    wire = topic(:pets, %{owner_id: 42})
    assert {:ok, :pets, %{owner_id: "42"}} = VerifiedPubSub.Info.match(Basic, wire)
  end

  test "a failed broadcast! names the wire topic it was sent on" do
    error =
      assert_raise RuntimeError, fn ->
        VerifiedPubSub.Broadcast.bang!(
          {:error, :boom},
          :pets,
          "owners:7:pets",
          :adopted
        )
      end

    assert Exception.message(error) =~ ~s|:adopted on :pets ("owners:7:pets"): :boom|
  end

  test "a param-free topic takes no params argument" do
    assert topic(:system) == "system"
  end

  test "broadcast_*! delivers a Message to subscribers of that topic", %{owner_id: id} do
    assert :ok = subscribe(:pets, %{owner_id: id})
    assert :ok = broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "c1"})

    assert_receive %Message{
      manifest: Basic,
      topic: :pets,
      event: :adopted,
      params: %{owner_id: ^id},
      payload: %{id: "c1"}
    }
  end

  test "broadcasts are scoped by param value", %{owner_id: id} do
    assert :ok = subscribe(:pets, %{owner_id: id})
    assert :ok = broadcast!(:pets, %{owner_id: id <> "other"}, :adopted, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "unsubscribe_* stops delivery", %{owner_id: id} do
    assert :ok = subscribe(:pets, %{owner_id: id})
    assert :ok = unsubscribe(:pets, %{owner_id: id})
    assert :ok = broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "the non-bang broadcast returns :ok" do
    assert :ok = broadcast(:pets, %{owner_id: "7"}, :adopted, %{id: "c1"})
  end

  test "a param-free topic broadcasts with only a payload" do
    # The :system topic has no params to make unique, and the suite shares one
    # Phoenix.PubSub, so the payload carries a token to keep concurrent tests on this
    # topic from matching each other's messages.
    token = unique_owner_id()
    assert :ok = subscribe(:system)
    assert :ok = broadcast!(:system, %{}, :alert, %{text: token})

    assert_receive %Message{topic: :system, event: :alert, params: %{}, payload: %{text: ^token}}
  end

  test "a params map built at runtime with a missing key raises KeyError" do
    # A literal map is caught at compile time (see VerifiedPubSub.ApiTest). This is the
    # dynamic case, which only Map.fetch!/2 can catch, at runtime.
    params = Map.new([{String.to_atom("wrong"), "7"}])

    assert_raise KeyError, fn ->
      broadcast!(:pets, params, :adopted, %{id: "c1"})
    end
  end

  test "broadcast_*_from! excludes the sender", %{owner_id: id} do
    assert :ok = subscribe(:pets, %{owner_id: id})

    assert :ok =
             broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "broadcast_*_from! still delivers to other subscribers", %{owner_id: id} do
    test_pid = self()

    other =
      spawn_link(fn ->
        subscribe(:pets, %{owner_id: id})
        send(test_pid, :ready)
        receive do: (%Message{payload: p} -> send(test_pid, {:other_got, p}))
      end)

    assert_receive :ready
    subscribe(:pets, %{owner_id: id})
    broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, %{id: "c1"})

    assert_receive {:other_got, %{id: "c1"}}
    refute_receive %Message{}, 50
    Process.exit(other, :kill)
  end

  test "the non-bang from variant returns :ok", %{owner_id: id} do
    assert :ok = broadcast_from(self(), :pets, %{owner_id: id}, :adopted, %{id: "c1"})
  end

  test "a param-free topic's from variant takes only from and payload" do
    token = unique_owner_id()
    assert :ok = subscribe(:system)
    assert :ok = broadcast_from!(self(), :system, %{}, :alert, %{text: token})

    refute_receive %Message{topic: :system, payload: %{text: ^token}}, 50
  end

  test "the from variant carries the same message shape as the base variant", %{
    owner_id: id
  } do
    test_pid = self()

    other =
      spawn_link(fn ->
        subscribe(:pets, %{owner_id: id})
        send(test_pid, :ready)
        receive do: (m -> send(test_pid, {:got, m}))
      end)

    assert_receive :ready
    broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, %{id: "c1"})

    assert_receive {:got,
                    %Message{
                      manifest: Basic,
                      topic: :pets,
                      event: :adopted,
                      params: %{owner_id: ^id},
                      payload: %{id: "c1"}
                    }}

    Process.exit(other, :kill)
  end

  describe "the params-free arities" do
    test "broadcast!/3 reaches subscribers of a param-free topic" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, :alert, %{text: token})

      assert_receive %Message{topic: :system, event: :alert, payload: %{text: ^token}}
    end

    test "broadcast/3 works too" do
      token = unique_owner_id()
      assert :ok = subscribe(:system, %{})
      assert :ok = broadcast(:system, :alert, %{text: token})

      assert_receive %Message{payload: %{text: ^token}}
    end

    test "broadcast_from!/4 skips the sender" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from!(self(), :system, :alert, %{text: token})

      refute_receive %Message{payload: %{text: ^token}}, 50
    end

    test "broadcast_from/4 skips the sender" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from(self(), :system, :alert, %{text: token})

      refute_receive %Message{payload: %{text: ^token}}, 50
    end

    test "the four-argument form still takes an explicit empty map" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, %{}, :alert, %{text: token})

      assert_receive %Message{payload: %{text: ^token}}
    end

    test "omitting params on a topic that takes them is a compile error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.NoParams")} do
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
          def go(payload), do: broadcast!(:pets, :adopted, payload)
        end
        """)

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "missing: [:owner_id]"
      assert message =~ "takes exactly [:owner_id]"
    end

    test "a forgotten payload is still caught as a non-atom event" do
      # broadcast!(:pets, %{...}, :adopted) is also arity 3, so the params map lands
      # in the event slot rather than being silently accepted.
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.NoPayload")} do
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
          def go(id), do: broadcast!(:pets, %{owner_id: id}, :adopted)
        end
        """)

      assert %CompileError{} = error
      assert Exception.message(error) =~ "expected a literal atom for event"
    end
  end
end
