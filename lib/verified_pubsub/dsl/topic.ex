defmodule VerifiedPubSub.Dsl.Topic do
  @moduledoc "A topic that a manifest declares, with its wire segments and its events."

  @typedoc "A segment of a topic. A string is a literal, and an atom is a param."
  @type segment :: String.t() | atom()

  @typedoc """
  The fields of the topic:

    * `name`: the name that call sites use.
    * `segments`: the segments of the wire topic.
    * `description`: the description, or `nil`.
    * `params`: the params, in the order of the segments.
    * `messages`: the events of the topic.
    * `__identifier__` and `__spark_metadata__`: Spark sets these.
  """
  @type t :: %__MODULE__{
          name: atom(),
          segments: [segment()],
          description: String.t() | nil,
          params: [atom()],
          messages: [VerifiedPubSub.Dsl.Message.t()]
        }

  defstruct [
    :name,
    :segments,
    :description,
    :__identifier__,
    params: [],
    messages: [],
    __spark_metadata__: nil
  ]
end
