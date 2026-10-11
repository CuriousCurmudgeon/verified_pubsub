defmodule VerifiedPubSub.DslTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Info
  alias VerifiedPubSub.TestManifests.Basic

  test "topics are parsed at the top level, without a wrapper block" do
    assert [:pets, :system, :typed_payloads] =
             Info.topics(Basic) |> Enum.map(& &1.name) |> Enum.sort()
  end

  test "topic/2 returns the topic by name" do
    assert {:ok, topic} = Info.topic(Basic, :pets)
    assert topic.segments == ["owners", :owner_id, "pets"]
  end

  test "topic/2 returns :error for an unknown topic" do
    assert :error = Info.topic(Basic, :nope)
  end

  test "topic!/2 raises for an unknown topic and names the known ones" do
    assert_raise ArgumentError, ~r/unknown topic :nope.*:pets/s, fn ->
      Info.topic!(Basic, :nope)
    end
  end

  test "topic!/2 shows each known topic's wire pattern" do
    error = assert_raise ArgumentError, fn -> Info.topic!(Basic, :nope) end
    assert Exception.message(error) =~ ~r/:pets\s+owners:<owner_id>:pets/
  end

  describe "match/2" do
    test "finds the topic a wire string belongs to, with its params" do
      assert {:ok, :pets, %{owner_id: "42"}} = Info.match(Basic, "owners:42:pets")
    end

    test "matches a param-free topic" do
      assert {:ok, :system, %{}} = Info.match(Basic, "system")
    end

    test "is :error for a wrong literal or segment count" do
      assert :error = Info.match(Basic, "acounts:42:pets")
      assert :error = Info.match(Basic, "owners:42")
      assert :error = Info.match(Basic, "owners:42:pets:extra")
    end

    test "is :error when a param segment is empty, which no broadcast builds" do
      assert :error = Info.match(Basic, "owners::pets")
    end

    test "tells apart topics that differ only in segment count" do
      alias VerifiedPubSub.TestManifests.Adjacent
      assert {:ok, :short, %{x: "1"}} = Info.match(Adjacent, "adj:1")
      assert {:ok, :long, %{x: "1"}} = Info.match(Adjacent, "adj:1:b")
    end
  end

  test "events/2 returns declared event names in declaration order" do
    assert [:adopted, :renamed, :rehomed] = Info.events(Basic, :pets)
    assert [:alert] = Info.events(Basic, :system)
  end

  test "payload fields are parsed and exposed but not enforced" do
    {:ok, topic} = Info.topic(Basic, :pets)
    adopted = Enum.find(topic.messages, &(&1.name == :adopted))

    assert [{:id, :string}, {:name, :string}] = Enum.map(adopted.fields, &{&1.name, &1.type})
  end

  test "the manifest records its Phoenix.PubSub name" do
    assert Basic.__verified_pubsub_name__() == VerifiedPubSub.TestPubSub
  end

  test "the :pubsub option is required" do
    error =
      VerifiedPubSub.CompileHelper.compile_error("""
      defmodule #{VerifiedPubSub.CompileHelper.unique_module("VPTest.NoPubSub")} do
        use VerifiedPubSub.Manifest

        topic :pets, ["pets"] do
          message :adopted do
          end
        end
      end
      """)

    assert error, "expected a missing :pubsub option to fail compilation"
    assert Exception.message(error) =~ "pubsub"
  end
end
