defmodule VerifiedPubSub.Dsl do
  @moduledoc "The Spark DSL extension backing `VerifiedPubSub.Manifest`."

  alias Spark.Builder.{Entity, Section}

  @field Entity.new(:field, VerifiedPubSub.Dsl.Field,
           describe: "A field that must be present in the event's payload.",
           args: [:name, :type],
           identifier: :name,
           schema: [
             name: [type: :atom, required: true, doc: "The field name."],
             type: [
               # Deliberately :any. The permitted types include `{:list, type}` tuples
               # and struct modules, which no Spark type captures, so
               # VerifiedPubSub.Transformers.ValidateFields checks this instead and can
               # report the valid options.
               type: :any,
               required: true,
               doc:
                 "A primitive (:string, :integer, :float, :boolean, :atom, :map, :list, :any), " <>
                   "a {:list, type} tuple, or a struct module."
             ],
             required: [
               type: :boolean,
               default: true,
               doc: "Whether the key must be present in the payload."
             ],
             description: [
               type: :string,
               doc: "What the field holds. Shown in the manifest's generated docs."
             ]
           ]
         )
         |> Entity.build!()

  @message Entity.new(:message, VerifiedPubSub.Dsl.Message,
             describe: "An event that can be broadcast on the enclosing topic.",
             args: [:name],
             identifier: :name,
             entities: [fields: [@field]],
             schema: [
               name: [type: :atom, required: true, doc: "The event name."],
               description: [
                 type: :string,
                 doc: "When the event is broadcast. Shown in the manifest's generated docs."
               ]
             ]
           )
           |> Entity.build!()

  @topic Entity.new(:topic, VerifiedPubSub.Dsl.Topic,
           describe: "A topic and the events that may be broadcast on it.",
           args: [:name, :segments],
           identifier: :name,
           entities: [messages: [@message]],
           schema: [
             name: [
               type: :atom,
               required: true,
               doc: "The name call sites use. Independent of the wire segments."
             ],
             # A list rather than an "owners:%{owner_id}:pets" string, so a param fills a
             # whole segment by construction and can never complete a literal into a
             # different topic. A string would need parsing and a whole-segment check; the
             # readable wire form it would give is rendered by Topic.pattern/1 instead.
             segments: [
               type: {:list, {:or, [:string, :atom]}},
               required: true,
               doc:
                 "Wire topic segments, joined with `:`. A string is a literal and an atom is " <>
                   ~S|a parameter, e.g. `["owners", :owner_id, "pets"]`.|
             ],
             # Every entity takes description as an option, but a topic or message with
             # a do block gives it inside the block: Spark has no macro for keyword
             # options alongside a do block, so `topic :x, [...], description: "..." do`
             # fails to compile. A field has no block, so it uses the keyword.
             description: [
               type: :string,
               doc: "What the topic carries. Shown in the manifest's generated docs."
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

  # Spark appends what this returns to the manifest's @moduledoc, after any hand-written
  # moduledoc, and skips a manifest with `@moduledoc false`.
  @impl Spark.Dsl.Extension
  def explain(dsl_state) do
    case Spark.Dsl.Transformer.get_entities(dsl_state, [:topics]) do
      [] -> nil
      topics -> VerifiedPubSub.Docs.topics_section(topics)
    end
  end
end
