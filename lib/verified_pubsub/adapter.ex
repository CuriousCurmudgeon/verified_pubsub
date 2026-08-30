defmodule VerifiedPubsub.Adapter do
  @moduledoc """
  Transport behaviour, so `verified_pubsub` does not require Phoenix.

  `config` is opaque to the library and comes from the `:pubsub` option given to
  `use VerifiedPubsub.Registry`.
  """

  alias VerifiedPubsub.Message

  @callback broadcast(config :: term(), topic :: String.t(), message :: Message.t()) ::
              :ok | {:error, term()}
  @callback subscribe(config :: term(), topic :: String.t()) :: :ok | {:error, term()}
  @callback unsubscribe(config :: term(), topic :: String.t()) :: :ok | {:error, term()}
end
