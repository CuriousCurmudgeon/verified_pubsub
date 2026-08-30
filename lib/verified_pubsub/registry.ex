defmodule VerifiedPubsub.Registry do
  @moduledoc """
  Declares the topics and events for an application.

      defmodule MyApp.Topics do
        use VerifiedPubsub.Registry,
          adapter: VerifiedPubsub.Adapter.PhoenixPubSub,
          pubsub: MyApp.PubSub

        topic :campaigns, "accounts:%{account_id}:campaigns" do
          message :created do
            field :id, :string
          end
        end
      end
  """

  use Spark.Dsl,
    default_extensions: [extensions: [VerifiedPubsub.Dsl]],
    opt_schema: [
      adapter: [
        type: {:behaviour, VerifiedPubsub.Adapter},
        required: true,
        doc: "The `VerifiedPubsub.Adapter` used to broadcast and subscribe."
      ],
      pubsub: [
        type: :any,
        default: nil,
        doc: "Opaque adapter config. For the Phoenix adapter, the `Phoenix.PubSub` name."
      ]
    ]

  @impl Spark.Dsl
  def handle_opts(opts) do
    quote do
      @doc false
      def __verified_pubsub_adapter__, do: unquote(opts[:adapter])

      @doc false
      def __verified_pubsub_config__, do: unquote(Macro.escape(opts[:pubsub]))
    end
  end
end
