defmodule VerifiedPubSub.Payload do
  @moduledoc """
  Validates the payload of each broadcast against the manifest at runtime.

  The check runs on each broadcast, in each environment. It costs one pass over the
  declared fields and one comparison of key sets. That cost is small next to the send to
  `Phoenix.PubSub` that it guards.
  """

  alias VerifiedPubSub.Dsl.Field
  alias VerifiedPubSub.PayloadError

  @doc """
  Checks `payload` against the fields that the manifest declares for `{topic, event}`.

  Raises `VerifiedPubSub.PayloadError` on any mismatch. Returns the payload unchanged, so
  a caller can use the call inline.
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
      fields
      |> Enum.filter(& &1.required)
      |> MapSet.new(& &1.name)

    missing =
      required
      |> MapSet.difference(present)
      |> Enum.sort()

    unexpected =
      present
      |> MapSet.difference(declared)
      |> Enum.sort()

    # Do not use a `for` here. In a `for`, a qualifier like `value = Map.fetch!(...)` is a
    # filter on truth. Each nil or false value would then skip its type check, also on a
    # required field.
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

  An optional field accepts `nil`, because Elixir and Ecto use `nil` for an absent value. A
  payload that comes from a struct holds every key, so a rule to leave out the key would be
  hard to meet. A required field rejects `nil`.
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
