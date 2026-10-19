defmodule VerifiedPubSub.PayloadError do
  @moduledoc """
  The error for a broadcast payload that does not match the shape that the manifest
  declares.

  `broadcast/4` and `broadcast!/4` both raise it. A shape violation is a bug in the code of
  the caller, not a failure that the caller can expect. So `{:error, _}` only reports a
  transport failure, which a caller can handle.

  The error reports every problem at once, not only the first:

      invalid payload for :pets :adopted in MyApp.Topics:

        * missing required key: :id
        * unexpected key: :extra — not declared on this event
        * :name is declared as :string, got: 42

  """

  defexception [:manifest, :topic, :event, :problems, :payload]

  @typedoc """
  A problem with a payload. Each value is one of these:

    * `{:not_a_map, value}`: the payload is not a map.
    * `{:struct_payload, module}`: the payload is a struct of `module`, not a map.
    * `{:missing, names}`: the payload has no key for these required fields.
    * `{:unexpected, names}`: the payload has keys that the event does not declare.
    * `{:type, name, type, value}`: the value of the field `name` does not match `type`.
  """
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
    field =
      module
      |> Module.split()
      |> List.last()
      |> Macro.underscore()

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
