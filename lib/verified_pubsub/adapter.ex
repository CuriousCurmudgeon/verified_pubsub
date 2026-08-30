defmodule VerifiedPubsub.Adapter do
  @moduledoc """
  Transport behaviour, so `verified_pubsub` does not require Phoenix.

  `config` is opaque to the library and comes from the `:pubsub` option given to
  `use VerifiedPubsub.Registry`.
  """

  alias VerifiedPubsub.Message

  @callback broadcast(config :: term(), topic :: String.t(), message :: Message.t()) ::
              :ok | {:error, term()}

  @doc """
  Broadcasts to every subscriber except `from`.

  Mirrors `Phoenix.PubSub.broadcast_from/4`, including its semantics: `from` is
  whichever pid the caller supplies, so `self()` refers to the calling process, not
  necessarily the one that started the request.

  Required rather than optional: an adapter that cannot exclude a subscriber should say
  so explicitly rather than silently fall back to `c:broadcast/3` and deliver the
  message to the sender anyway.
  """
  @callback broadcast_from(
              config :: term(),
              from :: pid(),
              topic :: String.t(),
              message :: Message.t()
            ) :: :ok | {:error, term()}
  @callback subscribe(config :: term(), topic :: String.t()) :: :ok | {:error, term()}
  @callback unsubscribe(config :: term(), topic :: String.t()) :: :ok | {:error, term()}
end
