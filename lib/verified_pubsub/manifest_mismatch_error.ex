defmodule VerifiedPubSub.ManifestMismatchError do
  @moduledoc """
  Raised when a subscriber receives a message declared by a different manifest.

  Nothing about such a message was verified against the subscriber's own manifest. Its
  topic and event happen to share names, and its payload follows the *sender's* field
  declarations, so dispatching it would hand a handler a payload shape the compiler never
  checked — which is the one thing this library exists to prevent.

  Two manifests can produce the same wire topic when they share a `:pubsub` and declare
  patterns that overlap. Disjointness is checked *within* a manifest, not across manifests,
  because a manifest cannot see its siblings at compile time.
  """

  defexception [:subscriber, :expected, :got, :topic, :event]

  @impl true
  def message(%__MODULE__{} = error) do
    """
    #{inspect(error.subscriber)} is bound to #{inspect(error.expected)} but received a \
    message declared by #{inspect(error.got)}:

      topic #{inspect(error.topic)}, event #{inspect(error.event)}

    Nothing about that message was verified against #{inspect(error.expected)}. Its payload \
    follows #{inspect(error.got)}'s field declarations, so it was not dispatched.

    Two manifests produce the same wire topic when they share a `:pubsub` and declare \
    patterns that can build the same string. Either give each manifest its own \
    `Phoenix.PubSub`, or make the patterns distinct.
    """
  end
end
