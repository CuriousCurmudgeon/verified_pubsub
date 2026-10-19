defmodule VerifiedPubSub.Transformers.DefinePayloadSchemas do
  @moduledoc """
  Defines `__verified_pubsub_fields__/2` on the manifest, with one clause for each event.

  Each clause takes a topic and an event, and returns the fields that the manifest
  declares for that event. A clause leaves out the `__spark_metadata__` of each field.
  That metadata holds source annotations, which only add size to the compiled manifest.

  ## Tradeoffs

  The manifest holds the fields, and each broadcast calls a clause to read them. This
  gives up a little speed: a copy of the fields at each call site is a little faster. But
  a module that Mix does not compile again after a change to the manifest keeps an old
  copy. Each copy also makes its call site larger. A clause that returns a literal list
  is fast enough to run on each broadcast.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer
  alias VerifiedPubSub.Dsl.Field

  @impl true
  def after?(VerifiedPubSub.Transformers.ValidateFields), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    clauses =
      for topic <- Transformer.get_entities(dsl, [:topics]), message <- topic.messages do
        fields =
          Enum.map(message.fields, fn field ->
            %Field{name: field.name, type: field.type, required: field.required}
          end)

        quote do
          @doc false
          def __verified_pubsub_fields__(unquote(topic.name), unquote(message.name)) do
            unquote(Macro.escape(fields))
          end
        end
      end

    # One eval defines every clause, so the clauses of the function stay together.
    {:ok, Transformer.eval(dsl, [], quote(do: (unquote_splicing(clauses))))}
  end
end
