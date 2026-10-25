defmodule VerifiedPubSub.Manifest do
  @moduledoc """
  Declares the topics and events of an application.

      defmodule MyApp.Topics do
        use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

        topic :pets, ["owners", :owner_id, "pets"] do
          message :adopted do
            field :id, :string
          end
        end
      end

  Broadcasts and subscriptions go through `Phoenix.PubSub`, so `:pubsub` names a
  `Phoenix.PubSub` that your supervision tree starts. Configure the transport, such as
  PG2 or Redis, on `Phoenix.PubSub` itself, not here.

  ## Topics

  `:pets` is the name that call sites use. The list is the wire topic, so a change to its
  segments does not change a call site. A `:` joins the segments. A string is a literal,
  and an atom is a param. The params come from the segments, so you do not declare them
  twice. `VerifiedPubSub.Topic` gives the rules for segments.

  ## Fields

  Each `field` declares a key that the payload must carry:

      message :adopted do
        field :id, :string
        field :pet, MyApp.Pet
        field :tags, {:list, :string}
        field :note, :string, required: false
      end

  A type is one of these:

    * `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list` or `:any`
    * a `{:list, type}` tuple
    * a struct module

  An unknown type is a compile error. With `required: false`, the key can be absent, or
  present as `nil`.

  **The payload is a map of exactly the declared fields.** A broadcast rejects an
  undeclared key, so the manifest stays an accurate description of the data on the wire.
  For the same reason, a struct cannot be the payload itself, because it carries
  `__struct__` and all of its own keys. Put the struct in a field:

      broadcast!(:pets, %{owner_id: id}, :adopted, %{pet: pet})

  ## Documentation

  The manifest adds a `## Topics` section to its `@moduledoc`. The section comes from the
  declarations: the wire pattern of each topic, its events, and the fields and types of
  each event. A `@moduledoc` that you write comes first. `@moduledoc false` keeps the
  manifest hidden.

  A `description` on a topic, an event, or a field adds its meaning to that section. Give
  it inside the block, or as a keyword when there is no block:

      topic :pets, ["owners", :owner_id, "pets"] do
        description "Pets of one owner."

        message :adopted do
          description "The owner adopted a pet."
          field :pet, MyApp.Pet, description: "The adopted pet."
        end

        message :rehomed, description: "The pet moved to a new owner."
      end

  `docs: true` turns on a `## PubSub` section for each module that binds the manifest. The
  section lists the topics and events that the module publishes and handles. The `docs`
  option of a `use` line overrides the manifest for one module. See `VerifiedPubSub`.
  """

  use Spark.Dsl,
    default_extensions: [extensions: [VerifiedPubSub.Dsl]],
    opt_schema: [
      pubsub: [
        type: :atom,
        required: true,
        doc: "The name of the `Phoenix.PubSub` process that broadcasts and subscriptions use."
      ],
      docs: [
        type: :boolean,
        default: false,
        doc:
          "Adds a `## PubSub` section to the moduledoc of each module that binds this manifest."
      ]
    ]

  @impl Spark.Dsl
  def handle_opts(opts) do
    quote do
      @doc false
      def __verified_pubsub_name__, do: unquote(opts[:pubsub])

      @doc false
      def __verified_pubsub_docs__, do: unquote(Keyword.get(opts, :docs, false))
    end
  end
end
