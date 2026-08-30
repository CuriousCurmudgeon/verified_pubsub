defmodule VerifiedPubsub.DslTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Info
  alias VerifiedPubsub.TestRegistries.Basic

  test "topics are parsed at the top level, without a wrapper block" do
    assert [:campaigns, :system] = Info.topics(Basic) |> Enum.map(& &1.name) |> Enum.sort()
  end

  test "topic/2 returns the topic by name" do
    assert {:ok, topic} = Info.topic(Basic, :campaigns)
    assert topic.pattern == "accounts:%{account_id}:campaigns"
  end

  test "topic/2 returns :error for an unknown topic" do
    assert :error = Info.topic(Basic, :nope)
  end

  test "topic!/2 raises for an unknown topic and names the known ones" do
    assert_raise ArgumentError, ~r/unknown topic :nope.*:campaigns/s, fn ->
      Info.topic!(Basic, :nope)
    end
  end

  test "events/2 returns declared event names in declaration order" do
    assert [:created, :updated, :deleted] = Info.events(Basic, :campaigns)
    assert [:alert] = Info.events(Basic, :system)
  end

  test "payload fields are parsed and exposed but not enforced" do
    {:ok, topic} = Info.topic(Basic, :campaigns)
    created = Enum.find(topic.messages, &(&1.name == :created))

    assert [{:id, :string}, {:name, :string}] = Enum.map(created.fields, &{&1.name, &1.type})
  end

  test "the registry records its adapter and config" do
    assert Basic.__verified_pubsub_adapter__() == VerifiedPubsub.Adapter.Local
    assert Basic.__verified_pubsub_config__() == nil
  end
end
