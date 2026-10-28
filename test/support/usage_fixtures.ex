defmodule VerifiedPubSub.UsageFixtures do
  @moduledoc "Modules that `mix verified_pubsub.topics` reports in its tests."

  defmodule Topics do
    @moduledoc false
    use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

    topic :kennel, ["kennels", :kennel_id] do
      message :checked_in do
        field :dog_id, :string
      end

      message :checked_out do
        field :dog_id, :string
      end
    end

    topic :walks, ["walks"] do
      message :started do
        field :dog_id, :string
      end
    end
  end

  defmodule Kennel do
    @moduledoc false
    use VerifiedPubSub, manifest: VerifiedPubSub.UsageFixtures.Topics

    def check_in(kennel_id, dog_id) do
      broadcast!(:kennel, %{kennel_id: kennel_id}, :checked_in, %{dog_id: dog_id})
    end

    def start_walk(dog_id), do: broadcast!(:walks, :started, %{dog_id: dog_id})
  end

  defmodule Board do
    @moduledoc false
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.UsageFixtures.Topics

    def watch(kennel_id), do: subscribe(:kennel, %{kennel_id: kennel_id})

    handle_message :kennel, :checked_in, _payload, state do
      {:noreply, state}
    end

    ignore_message :kennel, :checked_out
  end
end
