defmodule VerifiedPubsub.Dsl.Field do
  @moduledoc "A declared payload field. Parsed in pass 1, not enforced."

  @type t :: %__MODULE__{name: atom(), type: atom()}

  defstruct [:name, :type, :__identifier__, :__spark_metadata__]
end
