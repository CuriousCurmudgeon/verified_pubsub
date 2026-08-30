defmodule VerifiedPubsub.Dsl do
  @moduledoc "The Spark DSL extension backing `VerifiedPubsub.Registry`."

  alias Spark.Builder.{Entity, Section}

  @field Entity.new(:field, VerifiedPubsub.Dsl.Field,
           describe: "A payload field. Declared in pass 1; not yet enforced.",
           args: [:name, :type],
           identifier: :name,
           schema: [
             name: [type: :atom, required: true, doc: "The field name."],
             type: [type: :atom, required: true, doc: "The field type."]
           ]
         )
         |> Entity.build!()

  @message Entity.new(:message, VerifiedPubsub.Dsl.Message,
             describe: "An event that can be broadcast on the enclosing topic.",
             args: [:name],
             identifier: :name,
             entities: [fields: [@field]],
             schema: [name: [type: :atom, required: true, doc: "The event name."]]
           )
           |> Entity.build!()

  @topic Entity.new(:topic, VerifiedPubsub.Dsl.Topic,
           describe: "A topic and the events that may be broadcast on it.",
           args: [:name, :pattern],
           identifier: :name,
           entities: [messages: [@message]],
           schema: [
             name: [
               type: :atom,
               required: true,
               doc: "Alias used to build function names. Independent of the wire pattern."
             ],
             pattern: [
               type: :string,
               required: true,
               doc:
                 ~S|Wire topic. `%{name}` marks a parameter, e.g. "accounts:%{account_id}:campaigns".|
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
    transformers: [VerifiedPubsub.Transformers.ParseParams]
end
