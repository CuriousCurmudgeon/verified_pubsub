defmodule VerifiedPubsub.Dsl.Message do
  @moduledoc "A declared event on a topic."

  @type t :: %__MODULE__{name: atom(), fields: [VerifiedPubsub.Dsl.Field.t()]}

  defstruct [:name, :__identifier__, fields: [], __spark_metadata__: nil]
end
