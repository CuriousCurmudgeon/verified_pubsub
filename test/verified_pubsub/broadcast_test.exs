defmodule VerifiedPubSub.BroadcastTest do
  use ExUnit.Case, async: true
  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestManifests.Basic

  import VerifiedPubSub.CompileHelper

  setup do
    %{owner_id: unique_owner_id()}
  end

  describe "Info.match/2" do
    test "returns the name and the params of a wire topic that topic/2 builds" do
      wire = topic(:pets, %{owner_id: 42})
      assert {:ok, :pets, %{owner_id: "42"}} = VerifiedPubSub.Info.match(Basic, wire)
    end
  end

  describe "Broadcast.bang!/4" do
    test "the error of a failed broadcast names its wire topic" do
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
  end

  describe "unsubscribe/2" do
    test "stops delivery", %{owner_id: id} do
      assert :ok = subscribe(:pets, %{owner_id: id})
      assert :ok = unsubscribe(:pets, %{owner_id: id})
      assert :ok = broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "c1"})

      refute_receive %Message{}, 50
    end
  end

  describe "topic/1" do
    test "a topic that takes no params needs no params argument" do
      assert topic(:system) == "system"
    end
  end

  describe "topic/2" do
    test "puts the params into the wire topic" do
      assert topic(:pets, %{owner_id: "7"}) == "owners:7:pets"
    end

    test "converts params that are not binaries to strings" do
      assert topic(:pets, %{owner_id: 7}) == "owners:7:pets"
    end
  end

  describe "broadcast/3" do
    test "delivers on a topic that takes no params" do
      token = unique_owner_id()
      assert :ok = subscribe(:system, %{})
      assert :ok = broadcast(:system, :alert, %{text: token})

      assert_receive %Message{payload: %{text: ^token}}
    end
  end

  describe "broadcast/4" do
    test "returns :ok" do
      assert :ok = broadcast(:pets, %{owner_id: "7"}, :adopted, %{id: "c1"})
    end
  end

  describe "broadcast!/3" do
    test "reaches the subscribers of a topic that takes no params" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, :alert, %{text: token})

      assert_receive %Message{topic: :system, event: :alert, payload: %{text: ^token}}
    end

    test "a call without params on a topic that takes params is a compile error that names the missing params" do
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

    test "a call without a payload is a compile error that asks for a literal atom as the event" do
      # broadcast!(:pets, %{...}, :adopted) also has arity 3, so the params map goes in
      # the event slot. The macro rejects it, and does not accept it without notice.
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

  describe "broadcast!/4" do
    test "delivers a Message to the subscribers of that topic", %{owner_id: id} do
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

    test "reaches only the subscribers of the same param value", %{owner_id: id} do
      assert :ok = subscribe(:pets, %{owner_id: id})
      assert :ok = broadcast!(:pets, %{owner_id: id <> "other"}, :adopted, %{id: "c1"})

      refute_receive %Message{}, 50
    end

    test "a topic that takes no params broadcasts with only a payload" do
      # The :system topic has no params to make unique, and the suite shares one
      # Phoenix.PubSub. So the payload carries a token, and concurrent tests on this topic
      # do not match the messages of other tests.
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, %{}, :alert, %{text: token})

      assert_receive %Message{
        topic: :system,
        event: :alert,
        params: %{},
        payload: %{text: ^token}
      }
    end

    test "accepts an explicit empty map" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, %{}, :alert, %{text: token})

      assert_receive %Message{payload: %{text: ^token}}
    end

    test "a params map built at runtime with a missing key raises KeyError" do
      # The compile check finds a missing key in a literal map (see
      # VerifiedPubSub.ApiTest). This test covers a map built at runtime, which only
      # Map.fetch!/2 can check, at runtime.
      params = Map.new([{String.to_atom("wrong"), "7"}])

      assert_raise KeyError, fn ->
        broadcast!(:pets, params, :adopted, %{id: "c1"})
      end
    end
  end

  describe "broadcast_from/4" do
    test "skips the sender" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from(self(), :system, :alert, %{text: token})

      refute_receive %Message{payload: %{text: ^token}}, 50
    end
  end

  describe "broadcast_from/5" do
    test "returns :ok", %{owner_id: id} do
      assert :ok = broadcast_from(self(), :pets, %{owner_id: id}, :adopted, %{id: "c1"})
    end
  end

  describe "broadcast_from!/4" do
    test "skips the sender" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from!(self(), :system, :alert, %{text: token})

      refute_receive %Message{payload: %{text: ^token}}, 50
    end
  end

  describe "broadcast_from!/5" do
    test "excludes the sender", %{owner_id: id} do
      assert :ok = subscribe(:pets, %{owner_id: id})

      assert :ok =
               broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, %{id: "c1"})

      refute_receive %Message{}, 50
    end

    test "delivers to the other subscribers", %{owner_id: id} do
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

    test "a topic that takes no params needs only from and the payload" do
      token = unique_owner_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from!(self(), :system, %{}, :alert, %{text: token})

      refute_receive %Message{topic: :system, payload: %{text: ^token}}, 50
    end

    test "gives a message of the same shape as broadcast!/4", %{
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
  end
end
