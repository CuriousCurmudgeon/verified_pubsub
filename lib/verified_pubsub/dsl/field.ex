defmodule VerifiedPubSub.Dsl.Field do
  @moduledoc """
  A payload field that a manifest declares.

  The `type` is one of the primitives (`:string`, `:integer`, `:float`, `:boolean`,
  `:atom`, `:map`, `:list`, `:any`), a `{:list, type}` tuple, or a struct module.
  """

  @type type :: atom() | {:list, type()}
  @type t :: %__MODULE__{
          name: atom(),
          type: type(),
          required: boolean(),
          description: String.t() | nil
        }

  defstruct [:name, :type, :description, :__identifier__, :__spark_metadata__, required: true]
end
