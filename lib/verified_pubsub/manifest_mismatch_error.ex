defmodule VerifiedPubSub.ManifestMismatchError do
  @moduledoc """
  The error for a message that a subscriber receives from a different manifest.

  The compiler did not verify such a message against the manifest of the subscriber. Only
  the names of its topic and event match, and its payload follows the field declarations
  of the *sender*. A dispatch would give a handler a payload shape that the compiler did
  not check: the one thing that this library exists to prevent.

  Two manifests can produce the same wire topic when they share a `:pubsub` and declare
  topics that overlap. A manifest checks that its topics are disjoint only *within*
  itself, because it cannot see the other manifests at compile time.
  """

  alias VerifiedPubSub.Info

  @typedoc """
  The fields of the error:

    * `subscriber`: the subscriber that receives the message.
    * `expected`: the manifest that the subscriber is bound to.
    * `got`: the manifest that declares the message.
    * `topic`: the name of the topic of the message.
    * `event`: the event of the message.
    * `params`: the params of the message.
  """
  @type t :: %__MODULE__{
          subscriber: module(),
          expected: module(),
          got: module(),
          topic: atom(),
          event: atom(),
          params: map()
        }

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

  # Names the wire topic, and the topic of the subscriber that has the same wire topic.
  # The details come from the declaration of the sender, so a step can fail. A failed step
  # gives an empty string, because error text must not raise.
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
