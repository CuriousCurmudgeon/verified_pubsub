defmodule VerifiedPubSub.Transformers.ValidateFields do
  @moduledoc """
  Validates every declared field's type, at compile time.

  The Spark schema accepts `:any` for a field's type, because the permitted types include
  `{:list, type}` tuples and struct modules that no Spark type captures. Without this
  transformer `field :id, :strng` would be accepted silently and then match nothing at
  runtime.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @primitives [:string, :integer, :float, :boolean, :atom, :map, :list, :any]

  @doc "The primitive types a field may declare."
  def primitives, do: @primitives

  @doc """
  Whether `type` is a valid field type.

  A struct module is recognised syntactically, by its `Elixir.` prefix, rather than by
  loading it — the module may not be compiled yet when this runs.
  """
  def valid_type?(type) when type in @primitives, do: true
  def valid_type?({:list, inner}), do: valid_type?(inner)
  def valid_type?(type) when is_atom(type), do: struct_module?(type)
  def valid_type?(_type), do: false

  @doc "Whether `atom` names a module rather than a plain atom."
  def struct_module?(atom) when is_atom(atom) do
    atom |> Atom.to_string() |> String.starts_with?("Elixir.")
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
           module such as MyApp.Campaign.
           """,
           path: [:topics, topic, event, field.name],
           module: module
         )}
    end
  end
end
