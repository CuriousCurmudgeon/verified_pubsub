defmodule VerifiedPubSub.PayloadError do
  @moduledoc """
  Raised when a broadcast payload does not match the shape the manifest declares.

  Raised by both `broadcast/4` and `broadcast!/4`: a shape violation is a bug in the
  calling code, not an operational condition, so `{:error, _}` stays reserved for
  transport failures a caller might reasonably handle.

  Every problem is reported at once rather than the first:

      invalid payload for :campaigns :created in MyApp.Topics:

        * missing required key: :id
        * unexpected key: :extra — not declared on this event
        * :name is declared as :string, got: 42

  """

  defexception [:manifest, :topic, :event, :problems, :payload]

  @type problem ::
          {:not_a_map, term()}
          | {:struct_payload, module()}
          | {:missing, [atom()]}
          | {:unexpected, [atom()]}
          | {:type, atom(), VerifiedPubSub.Dsl.Field.type(), term()}

  @impl true
  def message(%__MODULE__{} = error) do
    """
    invalid payload for #{inspect(error.topic)} #{inspect(error.event)} in \
    #{inspect(error.manifest)}:

    #{Enum.map_join(error.problems, "\n", &describe(&1, error))}
    """
  end

  defp describe({:not_a_map, value}, _error) do
    "  * the payload must be a map of the declared fields, got: #{inspect(value)}"
  end

  defp describe({:struct_payload, module}, error) do
    field = module |> Module.split() |> List.last() |> Macro.underscore()

    """
      * the payload is a #{inspect(module)} struct, but a payload must be a map of the
        declared fields. Declare a field to carry it instead:

            message #{inspect(error.event)} do
              field :#{field}, #{inspect(module)}
            end

        and broadcast %{#{field}: value}.\
    """
  end

  defp describe({:missing, names}, _error) do
    "  * missing required #{keys(names)}"
  end

  defp describe({:unexpected, names}, _error) do
    "  * unexpected #{keys(names)} — not declared on this event"
  end

  defp describe({:type, name, type, value}, _error) do
    "  * #{inspect(name)} is declared as #{inspect(type)}, got: #{inspect(value)}"
  end

  defp keys([name]), do: "key: #{inspect(name)}"
  defp keys(names), do: "keys: #{inspect(names)}"
end
