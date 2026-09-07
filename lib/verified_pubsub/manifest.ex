defmodule VerifiedPubSub.Manifest do
  @moduledoc """
  Declares the topics and events for an application.

      defmodule MyApp.Topics do
        use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

        topic :campaigns, "accounts:%{account_id}:campaigns" do
          message :created do
            field :id, :string
          end
        end
      end

  Broadcasts and subscriptions go through `Phoenix.PubSub`, so `:pubsub` is the name of
  a `Phoenix.PubSub` started in your supervision tree. Transport choice (PG2, Redis, …)
  is configured there, on `Phoenix.PubSub` itself, rather than here.
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
