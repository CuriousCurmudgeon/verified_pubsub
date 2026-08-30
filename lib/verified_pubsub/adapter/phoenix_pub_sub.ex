if Code.ensure_loaded?(Phoenix.PubSub) do
  defmodule VerifiedPubsub.Adapter.PhoenixPubSub do
    @moduledoc """
    Adapter backed by `Phoenix.PubSub`. Defined only when `:phoenix_pubsub` is a
    dependency of the host application.

    `config` is the `Phoenix.PubSub` process name, given as the `:pubsub` option to
    `use VerifiedPubsub.Registry`.
    """

    @behaviour VerifiedPubsub.Adapter

    @impl true
    def subscribe(pubsub, topic) when is_binary(topic) do
      Phoenix.PubSub.subscribe(pubsub, topic)
    end

    @impl true
    def unsubscribe(pubsub, topic) when is_binary(topic) do
      Phoenix.PubSub.unsubscribe(pubsub, topic)
    end

    @impl true
    def broadcast(pubsub, topic, message) when is_binary(topic) do
      Phoenix.PubSub.broadcast(pubsub, topic, message)
    end
  end
end
