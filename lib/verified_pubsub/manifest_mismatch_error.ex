defmodule VerifiedPubSub.ManifestMismatchError do
  @moduledoc """
  Raised when a subscriber receives a message declared by a different manifest.

  Nothing about such a message was verified against the subscriber's own manifest. Its
  topic and event happen to share names, and its payload follows the *sender's* field
  declarations, so dispatching it would hand a handler a payload shape the compiler never
  checked — which is the one thing this library exists to prevent.

  Two manifests can produce the same wire topic when they share a `:pubsub` and declare
  topics that overlap. Disjointness is checked *within* a manifest, not across manifests,
  because a manifest cannot see its siblings at compile time.
  """

  alias VerifiedPubSub.Info

  defexception [:subscriber, :expected, :got, :topic, :event, params: %{}]

  @impl true
  def message(%__MODULE__{} = error) do
    """
    #{inspect(error.subscriber)} is bound to #{inspect(error.expected)} but received a \
    message declared by #{inspect(error.got)}:

      topic #{inspect(error.topic)}, event #{inspect(error.event)}#{collision(error)}

    Nothing about that message was verified against #{inspect(error.expected)}. Its payload \
    follows #{inspect(error.got)}'s field declarations, so it was not dispatched.

    Two manifests produce the same wire topic when they share a `:pubsub` and declare \
    topics that can build the same string. Either give each manifest its own \
    `Phoenix.PubSub`, or make the topics distinct.
    """
  end

  # Names the wire topic and which of the subscriber's own topics it collided with. Built
  # from the sender's declaration, so it is best effort: an error message must not raise.
  defp collision(error) do
    with {:ok, sent} <- Info.topic(error.got, error.topic),
         {:ok, ours, wire} <- match_wire(error, sent) do
      """

        sent as #{VerifiedPubSub.Topic.describe(sent)} by #{inspect(error.got)}
        wire topic #{inspect(wire)}
        matches #{VerifiedPubSub.Topic.describe(ours)} in #{inspect(error.expected)}\
      """
    else
      _ -> ""
    end
  rescue
    _ -> ""
  end

  defp match_wire(error, sent) do
    wire = build(sent.segments, error.params)

    case Info.match(error.expected, wire) do
      {:ok, ours, _params} -> {:ok, Info.topic!(error.expected, ours), wire}
      :error -> :error
    end
  end

  defp build(segments, params) do
    Enum.map_join(segments, VerifiedPubSub.Topic.separator(), fn
      literal when is_binary(literal) ->
        literal

      param ->
        params
        |> Map.get(param, "")
        |> to_string()
    end)
  end
end
