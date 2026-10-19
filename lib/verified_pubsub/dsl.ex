defmodule VerifiedPubSub.Dsl do
  @moduledoc "The Spark DSL extension that `VerifiedPubSub.Manifest` uses."

  alias Spark.Builder.{Entity, Section}

  @field Entity.new(:field, VerifiedPubSub.Dsl.Field,
           describe: "A field of the payload that the event carries.",
           args: [:name, :type],
           identifier: :name,
           schema: [
             name: [type: :atom, required: true, doc: "The name of the field."],
             type: [
               # The type is :any on purpose. No Spark type accepts a `{:list, type}`
               # tuple or a struct module, so VerifiedPubSub.Transformers.ValidateFields
               # checks the type. That check can also report the valid types.
               type: :any,
               required: true,
               doc:
                 "A primitive (:string, :integer, :float, :boolean, :atom, :map, :list, :any), " <>
                   "a {:list, type} tuple, or a struct module."
             ],
             required: [
               type: :boolean,
               default: true,
               doc: "Whether the payload must hold the field."
             ],
             description: [
               type: :string,
               doc: "What the field holds. The docs that the manifest generates show it."
             ]
           ]
         )
         |> Entity.build!()

  @message Entity.new(:message, VerifiedPubSub.Dsl.Message,
             describe: "An event that a call site can broadcast on the enclosing topic.",
             args: [:name],
             identifier: :name,
             entities: [fields: [@field]],
             schema: [
               name: [type: :atom, required: true, doc: "The name of the event."],
               description: [
                 type: :string,
                 doc:
                   "When a call site broadcasts the event. " <>
                     "The docs that the manifest generates show it."
               ]
             ]
           )
           |> Entity.build!()

  @topic Entity.new(:topic, VerifiedPubSub.Dsl.Topic,
           describe: "A topic and the events that a call site can broadcast on it.",
           args: [:name, :segments],
           identifier: :name,
           entities: [messages: [@message]],
           schema: [
             name: [
               type: :atom,
               required: true,
               doc:
                 "The name that call sites use. " <>
                   "It does not depend on the segments of the wire topic."
             ],
             # The segments are a list, not a string such as "owners:%{owner_id}:pets". In a
             # list, each param fills a whole segment, so a param value cannot join a literal
             # to make a different topic. A string needs a parser and a check for whole
             # segments. Topic.pattern/1 gives the readable wire form instead.
             segments: [
               type: {:list, {:or, [:string, :atom]}},
               required: true,
               doc:
                 "The segments of the wire topic, joined with `:`. A string is a literal, and " <>
                   ~S|an atom is a param, as in `["owners", :owner_id, "pets"]`.|
             ],
             # Every entity takes description as an option. Spark has no macro that takes
             # keyword options together with a do block, so
             # `topic :x, [...], description: "..." do` does not compile. A topic or a message
             # with a do block sets description inside the block. A field has no block, so it
             # uses the keyword option.
             description: [
               type: :string,
               doc: "What the topic carries. The docs that the manifest generates show it."
             ]
           ]
         )
         |> Entity.build!()

  @topics Section.new(:topics,
            describe: "Declares every topic and event in the application.",
            top_level?: true,
            entities: [@topic]
          )
          |> Section.build!()

  use Spark.Dsl.Extension,
    sections: [@topics],
    transformers: [
      VerifiedPubSub.Transformers.ParseParams,
      VerifiedPubSub.Transformers.ValidateTopics,
      VerifiedPubSub.Transformers.ValidateFields,
      VerifiedPubSub.Transformers.DefinePayloadSchemas
    ]

  # Spark adds the return value to the end of the @moduledoc of the manifest, after any
  # moduledoc that a person wrote. Spark skips a manifest with `@moduledoc false`.
  @impl Spark.Dsl.Extension
  def explain(dsl_state) do
    case Spark.Dsl.Transformer.get_entities(dsl_state, [:topics]) do
      [] -> nil
      topics -> VerifiedPubSub.Docs.topics_section(topics)
    end
  end
end
