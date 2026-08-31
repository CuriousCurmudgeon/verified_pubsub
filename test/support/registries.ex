defmodule VerifiedPubSub.TestRegistries do
  @moduledoc "Registries compiled in the test env and reused across test files."

  defmodule Basic do
    use VerifiedPubSub.Registry, pubsub: VerifiedPubSub.TestPubSub

    topic :campaigns, "accounts:%{account_id}:campaigns" do
      message :created do
        field :id, :string
        field :name, :string, required: false
      end

      message :updated do
        field :id, :string
      end

      message :deleted do
        field :id, :string
      end
    end

    topic :system, "system" do
      message :alert do
        field :text, :string
      end
    end

    # Exercises every field type and both required settings.
    topic :shapes, "shapes:%{owner_id}" do
      message :typed do
        field :name, :string
        field :count, :integer
        field :ratio, :float
        field :flag, :boolean
        field :kind, :atom
        field :meta, :map
        field :tags, {:list, :string}
        field :anything, :any
        field :note, :string, required: false
      end

      message :structured do
        field :point, VerifiedPubSub.TestStructs.Point
      end

      message :bare do
        field :id, :string
      end
    end
  end

  # Two patterns that are legitimately disjoint -- different segment counts -- but whose
  # topic strings collide if a param value is allowed to carry the separator.
  defmodule Adjacent do
    use VerifiedPubSub.Registry, pubsub: VerifiedPubSub.TestPubSub

    topic :short, "adj:%{x}" do
      message :ping do
        field :tag, :string
      end
    end

    topic :long, "adj:%{x}:b" do
      message :pong do
        field :tag, :string
      end
    end
  end
end
