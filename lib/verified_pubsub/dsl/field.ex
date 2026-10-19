defmodule VerifiedPubSub.Dsl.Field do
  @moduledoc "A payload field that a manifest declares."

  @typedoc """
  The type of a field. Each value is one of these:

    * a primitive: `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list`,
      or `:any`
    * a `{:list, type}` tuple
    * a struct module
  """
  @type type :: atom() | {:list, type()}

  @typedoc """
  The fields of the field:

    * `name`: the key of the field in the payload.
    * `type`: the type of the value.
    * `required`: whether the payload must hold the key with a value that is not `nil`.
    * `description`: the description, or `nil`.
    * `__identifier__` and `__spark_metadata__`: Spark sets these.
  """
  @type t :: %__MODULE__{
          name: atom(),
          type: type(),
          required: boolean(),
          description: String.t() | nil
        }

  defstruct [:name, :type, :description, :__identifier__, :__spark_metadata__, required: true]
end
