defmodule VerifiedPubSub.Dsl.Topic do
  @moduledoc "A declared topic, its wire segments, and its events."

  @type segment :: String.t() | atom()

  @type t :: %__MODULE__{
          name: atom(),
          segments: [segment()],
          params: [atom()],
          messages: [VerifiedPubSub.Dsl.Message.t()]
        }

  defstruct [:name, :segments, :__identifier__, params: [], messages: [], __spark_metadata__: nil]
end
