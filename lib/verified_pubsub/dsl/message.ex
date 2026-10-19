defmodule VerifiedPubSub.Dsl.Message do
  @moduledoc "An event that a manifest declares on a topic."

  @typedoc """
  The fields of the event:

    * `name`: the name of the event.
    * `description`: the description, or `nil`.
    * `fields`: the fields of the payload.
    * `__identifier__` and `__spark_metadata__`: Spark sets these.
  """
  @type t :: %__MODULE__{
          name: atom(),
          description: String.t() | nil,
          fields: [VerifiedPubSub.Dsl.Field.t()]
        }

  defstruct [:name, :description, :__identifier__, fields: [], __spark_metadata__: nil]
end
