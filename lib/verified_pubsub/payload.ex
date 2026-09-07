defmodule VerifiedPubSub.Payload do
  @moduledoc """
  Runtime validation of broadcast payloads against the manifest.

  Runs on every broadcast, in every environment. The cost is one pass over the declared
  fields plus a key-set comparison, which is negligible beside the PubSub send it guards.
  """

  alias VerifiedPubSub.Dsl.Field
  alias VerifiedPubSub.PayloadError

  @doc """
  Checks `payload` against the fields the manifest declares for `{topic, event}`,
  raising `VerifiedPubSub.PayloadError` on any mismatch.

  Returns the payload unchanged so it can be used inline.
  """
  @spec validate!(module(), atom(), atom(), term()) :: term()
  def validate!(manifest, topic, event, payload) do
    fields = manifest.__verified_pubsub_fields__(topic, event)

    case problems(fields, payload) do
      [] ->
        payload

      problems ->
        raise PayloadError,
          manifest: manifest,
          topic: topic,
          event: event,
          payload: payload,
          problems: problems
    end
  end

  defp problems(_fields, %{__struct__: module}), do: [{:struct_payload, module}]

  defp problems(fields, payload) when is_map(payload) do
    declared = MapSet.new(fields, & &1.name)
    present = MapSet.new(Map.keys(payload))

    required =
      fields |> Enum.filter(& &1.required) |> MapSet.new(& &1.name)

    missing = required |> MapSet.difference(present) |> Enum.sort()
    unexpected = present |> MapSet.difference(declared) |> Enum.sort()

    # Note the shape here: `value = Map.fetch!(...)` as a `for` qualifier would be
    # evaluated for truthiness and act as a filter, so every nil or false value would
    # skip its type check -- including on required fields.
    type_problems =
      fields
      |> Enum.filter(&Map.has_key?(payload, &1.name))
      |> Enum.reject(&valid_field?(&1, Map.fetch!(payload, &1.name)))
      |> Enum.map(&{:type, &1.name, &1.type, Map.fetch!(payload, &1.name)})

    Enum.concat([
      if(missing == [], do: [], else: [{:missing, missing}]),
      if(unexpected == [], do: [], else: [{:unexpected, unexpected}]),
      type_problems
    ])
  end

  defp problems(_fields, payload), do: [{:not_a_map, payload}]

  @doc """
  Whether `value` satisfies `field`.

  `nil` is accepted for an optional field: it is how Elixir and Ecto spell absence, and
  requiring the key to be omitted instead would be awkward when a payload is built from
  a struct. A required field rejects `nil`.
  """
  @spec valid_field?(Field.t(), term()) :: boolean()
  def valid_field?(%{required: false}, nil), do: true
  def valid_field?(field, value), do: valid?(field.type, value)

  @doc false
  def valid?(:any, _value), do: true
  def valid?(:string, value), do: is_binary(value)
  def valid?(:integer, value), do: is_integer(value)
  def valid?(:float, value), do: is_float(value)
  def valid?(:boolean, value), do: is_boolean(value)
  def valid?(:atom, value), do: is_atom(value)
  def valid?(:map, value), do: is_map(value)
  def valid?(:list, value), do: is_list(value)

  def valid?({:list, inner}, value) do
    is_list(value) and Enum.all?(value, &valid?(inner, &1))
  end

  def valid?(module, value) when is_atom(module), do: is_struct(value, module)
end
