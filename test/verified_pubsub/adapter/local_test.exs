defmodule VerifiedPubsub.Adapter.LocalTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Adapter.Local
  alias VerifiedPubsub.Message

  setup do
    start_supervised!(Local)
    :ok
  end

  defp message(payload) do
    %Message{
      registry: MyRegistry,
      topic: :campaigns,
      event: :created,
      params: %{account_id: "7"},
      payload: payload
    }
  end

  test "a subscriber receives messages broadcast on its topic" do
    assert :ok = Local.subscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))

    assert_receive %Message{event: :created, payload: %{id: "c1"}}
  end

  test "a subscriber receives nothing for a topic it did not subscribe to" do
    assert :ok = Local.subscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.broadcast(nil, "accounts:9:campaigns", message(%{id: "c1"}))

    refute_receive %Message{}, 50
  end

  test "unsubscribe stops delivery" do
    assert :ok = Local.subscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.unsubscribe(nil, "accounts:7:campaigns")
    assert :ok = Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))

    refute_receive %Message{}, 50
  end

  test "broadcasting to a topic with no subscribers is :ok" do
    assert :ok = Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))
  end

  test "every subscriber to a topic receives the message" do
    test_pid = self()

    other =
      spawn_link(fn ->
        Local.subscribe(nil, "accounts:7:campaigns")
        send(test_pid, :ready)
        receive do: (%Message{payload: p} -> send(test_pid, {:other_got, p}))
      end)

    assert_receive :ready
    Local.subscribe(nil, "accounts:7:campaigns")
    Local.broadcast(nil, "accounts:7:campaigns", message(%{id: "c1"}))

    assert_receive %Message{payload: %{id: "c1"}}
    assert_receive {:other_got, %{id: "c1"}}
    Process.exit(other, :kill)
  end
end
