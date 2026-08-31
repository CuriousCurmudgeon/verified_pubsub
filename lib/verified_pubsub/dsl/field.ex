defmodule VerifiedPubSub.Dsl.Field do
  @moduledoc """
  A declared payload field.

  The `type` is one of the primitives (`:string`, `:integer`, `:float`, `:boolean`,
  `:atom`, `:map`, `:list`, `:any`), a `{:list, type}` tuple, or a struct module.
  """

  @type type :: atom() | {:list, type()}
  @type t :: %__MODULE__{name: atom(), type: type(), required: boolean()}

  defstruct [:name, :type, :__identifier__, :__spark_metadata__, required: true]
end
