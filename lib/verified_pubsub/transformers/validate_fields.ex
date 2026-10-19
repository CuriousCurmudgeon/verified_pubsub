defmodule VerifiedPubSub.Transformers.ValidateFields do
  @moduledoc """
  Validates the type of each declared field at compile time.

  The Spark schema gives the type of a field the Spark type `:any`, because no Spark type
  accepts a `{:list, type}` tuple or a struct module. Without this check,
  `field :id, :strng` compiles with no error, and the field then matches no value at
  runtime.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @primitives [:string, :integer, :float, :boolean, :atom, :map, :list, :any]

  @doc "The primitive types that a manifest can give a field."
  @spec primitives() :: [atom()]
  def primitives, do: @primitives

  @doc """
  Whether `type` is a valid field type.

  The compiler may not have compiled a struct module yet when this check runs. So the
  check finds a struct module by its `Elixir.` prefix, and does not load the module.
  """
  @spec valid_type?(term()) :: boolean()
  def valid_type?(type) when type in @primitives, do: true
  def valid_type?({:list, inner}), do: valid_type?(inner)
  def valid_type?(type) when is_atom(type), do: struct_module?(type)
  def valid_type?(_type), do: false

  @doc "Whether `atom` names a module rather than a plain atom."
  @spec struct_module?(atom()) :: boolean()
  def struct_module?(atom) when is_atom(atom) do
    atom
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end

  @impl true
  def after?(VerifiedPubSub.Transformers.ValidateTopics), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    module = Transformer.get_persisted(dsl, :module)

    dsl
    |> Transformer.get_entities([:topics])
    |> Enum.flat_map(fn topic ->
      Enum.flat_map(topic.messages, fn message ->
        Enum.map(message.fields, &{topic.name, message.name, &1})
      end)
    end)
    |> Enum.find(fn {_t, _e, field} -> not valid_type?(field.type) end)
    |> case do
      nil ->
        {:ok, dsl}

      {topic, event, field} ->
        {:error,
         Spark.Error.DslError.exception(
           message: """
           invalid type #{inspect(field.type)} for field #{inspect(field.name)}.

           Valid types are #{inspect(@primitives)}, a {:list, type} tuple, or a struct
           module such as MyApp.Pet.
           """,
           path: [:topics, topic, event, field.name],
           module: module,
           location: VerifiedPubSub.Topic.anno(field)
         )}
    end
  end
end
