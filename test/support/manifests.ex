defmodule VerifiedPubSub.TestManifests do
  @moduledoc "Manifests compiled in the test env and reused across test files."

  defmodule Basic do
    use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

    topic :pets, ["owners", :owner_id, "pets"] do
      message :adopted do
        field :id, :string
        field :name, :string, required: false
      end

      message :renamed do
        field :id, :string
      end

      message :rehomed do
        field :id, :string
      end
    end

    topic :system, ["system"] do
      message :alert do
        field :text, :string
      end
    end

    # Uses every field type, and both values of `required`.
    topic :typed_payloads, ["payloads", :x] do
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

  # Turns on the PubSub section for each module that binds it.
  defmodule Documented do
    use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub, docs: true

    topic :system, ["system"] do
      message :alert do
        field :text, :string
      end
    end
  end

  # Two topics with different segment counts, so they are disjoint. But their wire topics
  # are the same when a param value can contain the separator.
  defmodule Adjacent do
    use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

    topic :short, ["adj", :x] do
      message :ping do
        field :tag, :string
      end
    end

    topic :long, ["adj", :x, "b"] do
      message :pong do
        field :tag, :string
      end
    end
  end

  # Two manifests that share a pubsub and declare the *same* wire pattern with different
  # payload fields. A manifest checks that its topics are disjoint only within itself, so
  # this compiles. This is how a message can reach a subscriber that is bound to the other
  # manifest.
  defmodule CollideA do
    use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

    topic :pets, ["collide", :owner_id, "pets"] do
      message :adopted do
        field :id, :string
      end
    end
  end

  defmodule CollideB do
    use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

    topic :pets, ["collide", :owner_id, "pets"] do
      message :adopted do
        field :species, :atom
      end
    end
  end
end
