defmodule VerifiedPubSub.Dsl.Topic do
  @moduledoc "A topic that a manifest declares, with its wire segments and its events."

  @type segment :: String.t() | atom()

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
