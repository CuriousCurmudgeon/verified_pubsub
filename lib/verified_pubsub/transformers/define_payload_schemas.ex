defmodule VerifiedPubSub.Transformers.DefinePayloadSchemas do
  @moduledoc """
  Generates `__verified_pubsub_fields__/2` on the manifest: one clause per
  `{topic, event}`, returning that event's declared fields.

  The schema lives on the manifest rather than being inlined into each broadcast site.
  Inlining would be marginally faster but would go stale in any caller that is not
  recompiled after a manifest change, and it would bloat every call site. A clause
  returning a literal list is cheap enough to run on every broadcast.

  The `__spark_metadata__` on each field is dropped: it carries source annotations that
  would be dead weight in the compiled manifest.
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

    # Emitted in one eval so the clauses land together.
    {:ok, Transformer.eval(dsl, [], quote(do: (unquote_splicing(clauses))))}
  end
end
