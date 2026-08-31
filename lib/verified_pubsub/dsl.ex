defmodule VerifiedPubSub.Dsl do
  @moduledoc "The Spark DSL extension backing `VerifiedPubSub.Registry`."

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
             ]
           ]
         )
         |> Entity.build!()

  @message Entity.new(:message, VerifiedPubSub.Dsl.Message,
             describe: "An event that can be broadcast on the enclosing topic.",
             args: [:name],
             identifier: :name,
             entities: [fields: [@field]],
             schema: [name: [type: :atom, required: true, doc: "The event name."]]
           )
           |> Entity.build!()

  @topic Entity.new(:topic, VerifiedPubSub.Dsl.Topic,
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
    transformers: [
      VerifiedPubSub.Transformers.ParseParams,
      VerifiedPubSub.Transformers.ValidateTopics,
      VerifiedPubSub.Transformers.ValidateFields,
      VerifiedPubSub.Transformers.DefinePayloadSchemas
    ]
end
