defmodule VerifiedPubSub.Dsl.Message do
  @moduledoc "A declared event on a topic."

  @type t :: %__MODULE__{
          name: atom(),
          description: String.t() | nil,
          fields: [VerifiedPubSub.Dsl.Field.t()]
        }

  defstruct [:name, :description, :__identifier__, fields: [], __spark_metadata__: nil]
end
