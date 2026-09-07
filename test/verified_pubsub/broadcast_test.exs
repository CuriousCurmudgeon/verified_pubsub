defmodule VerifiedPubSub.BroadcastTest do
  use ExUnit.Case, async: true

  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestManifests.Basic

  setup do
    %{account_id: unique_account_id()}
  end

  test "topic/2 interpolates params into the wire pattern" do
    assert topic(:campaigns, %{account_id: "7"}) == "accounts:7:campaigns"
  end

  test "topic/2 stringifies non-binary params" do
    assert topic(:campaigns, %{account_id: 7}) == "accounts:7:campaigns"
  end

  test "a param-free topic takes no params argument" do
    assert topic(:system) == "system"
  end

  test "broadcast_*! delivers a Message to subscribers of that topic", %{account_id: id} do
    assert :ok = subscribe(:campaigns, %{account_id: id})
    assert :ok = broadcast!(:campaigns, %{account_id: id}, :created, %{id: "c1"})

    assert_receive %Message{
      manifest: Basic,
      topic: :campaigns,
      event: :created,
      params: %{account_id: ^id},
      payload: %{id: "c1"}
    }
  end

  test "broadcasts are scoped by param value", %{account_id: id} do
    assert :ok = subscribe(:campaigns, %{account_id: id})
    assert :ok = broadcast!(:campaigns, %{account_id: id <> "other"}, :created, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "unsubscribe_* stops delivery", %{account_id: id} do
    assert :ok = subscribe(:campaigns, %{account_id: id})
    assert :ok = unsubscribe(:campaigns, %{account_id: id})
    assert :ok = broadcast!(:campaigns, %{account_id: id}, :created, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "the non-bang broadcast returns :ok" do
    assert :ok = broadcast(:campaigns, %{account_id: "7"}, :created, %{id: "c1"})
  end

  test "a param-free topic broadcasts with only a payload" do
    # The :system topic has no params to make unique, and the suite shares one
    # Phoenix.PubSub, so the payload carries a token to keep concurrent tests on this
    # topic from matching each other's messages.
    token = unique_account_id()
    assert :ok = subscribe(:system)
    assert :ok = broadcast!(:system, %{}, :alert, %{text: token})

    assert_receive %Message{topic: :system, event: :alert, params: %{}, payload: %{text: ^token}}
  end

  test "a params map built at runtime with a missing key raises KeyError" do
    # A literal map is caught at compile time (see VerifiedPubSub.ApiTest). This is the
    # dynamic case, which only Map.fetch!/2 can catch, at runtime.
    params = Map.new([{String.to_atom("wrong"), "7"}])

    assert_raise KeyError, fn ->
      broadcast!(:campaigns, params, :created, %{id: "c1"})
    end
  end

  test "broadcast_*_from! excludes the sender", %{account_id: id} do
    assert :ok = subscribe(:campaigns, %{account_id: id})

    assert :ok =
             broadcast_from!(self(), :campaigns, %{account_id: id}, :created, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "broadcast_*_from! still delivers to other subscribers", %{account_id: id} do
    test_pid = self()

    other =
      spawn_link(fn ->
        subscribe(:campaigns, %{account_id: id})
        send(test_pid, :ready)
        receive do: (%Message{payload: p} -> send(test_pid, {:other_got, p}))
      end)

    assert_receive :ready
    subscribe(:campaigns, %{account_id: id})
    broadcast_from!(self(), :campaigns, %{account_id: id}, :created, %{id: "c1"})

    assert_receive {:other_got, %{id: "c1"}}
    refute_receive %Message{}, 50
    Process.exit(other, :kill)
  end

  test "the non-bang from variant returns :ok", %{account_id: id} do
    assert :ok = broadcast_from(self(), :campaigns, %{account_id: id}, :created, %{id: "c1"})
  end

  test "a param-free topic's from variant takes only from and payload" do
    token = unique_account_id()
    assert :ok = subscribe(:system)
    assert :ok = broadcast_from!(self(), :system, %{}, :alert, %{text: token})

    refute_receive %Message{topic: :system, payload: %{text: ^token}}, 50
  end

  test "the from variant carries the same message shape as the base variant", %{
    account_id: id
  } do
    test_pid = self()

    other =
      spawn_link(fn ->
        subscribe(:campaigns, %{account_id: id})
        send(test_pid, :ready)
        receive do: (m -> send(test_pid, {:got, m}))
      end)

    assert_receive :ready
    broadcast_from!(self(), :campaigns, %{account_id: id}, :created, %{id: "c1"})

    assert_receive {:got,
                    %Message{
                      manifest: Basic,
                      topic: :campaigns,
                      event: :created,
                      params: %{account_id: ^id},
                      payload: %{id: "c1"}
                    }}

    Process.exit(other, :kill)
  end

  describe "the params-free arities" do
    test "broadcast!/3 reaches subscribers of a param-free topic" do
      token = unique_account_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, :alert, %{text: token})

      assert_receive %Message{topic: :system, event: :alert, payload: %{text: ^token}}
    end

    test "broadcast/3 works too" do
      token = unique_account_id()
      assert :ok = subscribe(:system, %{})
      assert :ok = broadcast(:system, :alert, %{text: token})

      assert_receive %Message{payload: %{text: ^token}}
    end

    test "broadcast_from!/4 skips the sender" do
      token = unique_account_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from!(self(), :system, :alert, %{text: token})

      refute_receive %Message{payload: %{text: ^token}}, 50
    end

    test "broadcast_from/4 skips the sender" do
      token = unique_account_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast_from(self(), :system, :alert, %{text: token})

      refute_receive %Message{payload: %{text: ^token}}, 50
    end

    test "the four-argument form still takes an explicit empty map" do
      token = unique_account_id()
      assert :ok = subscribe(:system)
      assert :ok = broadcast!(:system, %{}, :alert, %{text: token})

      assert_receive %Message{payload: %{text: ^token}}
    end

    test "omitting params on a topic that takes them is a compile error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.NoParams")} do
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
          def go(payload), do: broadcast!(:campaigns, :created, payload)
        end
        """)

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "missing: [:account_id]"
      assert message =~ "takes exactly [:account_id]"
    end

    test "a forgotten payload is still caught as a non-atom event" do
      # broadcast!(:campaigns, %{...}, :created) is also arity 3, so the params map lands
      # in the event slot rather than being silently accepted.
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.NoPayload")} do
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
          def go(id), do: broadcast!(:campaigns, %{account_id: id}, :created)
        end
        """)

      assert %CompileError{} = error
      assert Exception.message(error) =~ "expected a literal atom for event"
    end
  end
end
