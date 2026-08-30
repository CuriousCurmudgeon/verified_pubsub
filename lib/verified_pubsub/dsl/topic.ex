defmodule VerifiedPubsub.Dsl.Topic do
  @moduledoc "A declared topic, its wire pattern, and its events."

  @type t :: %__MODULE__{
          name: atom(),
          pattern: String.t(),
          params: [atom()],
          messages: [VerifiedPubsub.Dsl.Message.t()]
        }

  defstruct [:name, :pattern, :__identifier__, params: [], messages: [], __spark_metadata__: nil]
end
