defmodule VerifiedPubsub.TestRegistries do
  @moduledoc "Registries compiled in the test env and reused across test files."

  defmodule Basic do
    use VerifiedPubsub.Registry, adapter: VerifiedPubsub.Adapter.Local

    topic :campaigns, "accounts:%{account_id}:campaigns" do
      message :created do
        field(:id, :string)
        field(:name, :string)
      end

      message :updated do
        field(:id, :string)
      end

      message :deleted do
        field(:id, :string)
      end
    end

    topic :system, "system" do
      message :alert do
        field(:text, :string)
      end
    end
  end
end
