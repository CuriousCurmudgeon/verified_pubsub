defmodule VerifiedPubSub.Manifest do
  @moduledoc """
  Declares the topics and events for an application.

      defmodule MyApp.Topics do
        use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

        topic :pets, ["owners", :owner_id, "pets"] do
          message :adopted do
            field :id, :string
          end
        end
      end

  Broadcasts and subscriptions go through `Phoenix.PubSub`, so `:pubsub` is the name of
  a `Phoenix.PubSub` started in your supervision tree. Transport choice (PG2, Redis, …)
  is configured there, on `Phoenix.PubSub` itself, rather than here.

  ## Topics

  `:pets` is the name call sites use; the list is the wire topic, so changing its
  segments never touches a call site. Segments are joined with `:`. A string is a literal
  and an atom is a parameter, and the parameter list is derived from the segments rather
  than declared twice — see `VerifiedPubSub.Topic` for the rules segments must satisfy.

  ## Fields

  Each `field` declares a key the payload must carry:

      message :adopted do
        field :id, :string
        field :pet, MyApp.Pet
        field :tags, {:list, :string}
        field :note, :string, required: false
      end

  A type is one of `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list`,
  `:any`, a `{:list, type}` tuple, or a struct module. An unknown type is a compile error.
  `required: false` allows the key to be absent, or present as `nil`.

  **The payload is a map of exactly the declared fields.** Undeclared keys are rejected,
  which keeps the manifest an accurate description of what is on the wire. It also means a
  struct cannot be the payload itself — it carries `__struct__` and every one of its own
  keys — so put it in a field:

      broadcast!(:pets, %{owner_id: id}, :adopted, %{pet: pet})
  """

  use Spark.Dsl,
    default_extensions: [extensions: [VerifiedPubSub.Dsl]],
    opt_schema: [
      pubsub: [
        type: :atom,
        required: true,
        doc: "The `Phoenix.PubSub` process name that broadcasts and subscriptions go through."
      ]
    ]

  @impl Spark.Dsl
  def handle_opts(opts) do
    quote do
      @doc false
      def __verified_pubsub_name__, do: unquote(opts[:pubsub])
    end
  end
end
