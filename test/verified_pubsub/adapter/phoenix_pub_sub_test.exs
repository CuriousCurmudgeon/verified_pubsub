defmodule VerifiedPubsub.Adapter.PhoenixPubSubTest do
  use ExUnit.Case, async: true

  import VerifiedPubsub.CompileHelper

  alias VerifiedPubsub.Adapter.PhoenixPubSub
  alias VerifiedPubsub.Message

  setup do
    name = :"pubsub_#{System.unique_integer([:positive])}"
    start_supervised!({Phoenix.PubSub, name: name})
    %{pubsub: name}
  end

  defp message do
    %Message{
      registry: SomeRegistry,
      topic: :campaigns,
      event: :created,
      params: %{},
      payload: %{id: "c1"}
    }
  end

  test "delivers to subscribers", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast(pubsub, "t", message())

    assert_receive %Message{payload: %{id: "c1"}}
  end

  test "does not deliver to other topics", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast(pubsub, "other", message())

    refute_receive %Message{}, 50
  end

  test "unsubscribe stops delivery", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.unsubscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast(pubsub, "t", message())

    refute_receive %Message{}, 50
  end

  test "broadcast_from excludes the sender", %{pubsub: pubsub} do
    assert :ok = PhoenixPubSub.subscribe(pubsub, "t")
    assert :ok = PhoenixPubSub.broadcast_from(pubsub, self(), "t", message())

    refute_receive %Message{}, 50
  end

  test "a registry can use the Phoenix adapter end to end", %{pubsub: pubsub} do
    module =
      compile!("""
      defmodule #{unique_module("VPTest.PhoenixReg")} do
        use VerifiedPubsub.Registry,
          adapter: VerifiedPubsub.Adapter.PhoenixPubSub,
          pubsub: #{inspect(pubsub)}

        topic :campaigns, "accounts:%{account_id}:campaigns" do
          message :created do
            field :id, :string
          end
        end
      end
      """)

    assert :ok = module.subscribe_campaigns(%{account_id: "7"})
    assert :ok = module.broadcast_campaigns_created!(%{account_id: "7"}, %{id: "c1"})

    assert_receive %Message{topic: :campaigns, event: :created, params: %{account_id: "7"}}
  end
end
