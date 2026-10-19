defmodule VerifiedPubSub.DslTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Info
  alias VerifiedPubSub.TestManifests.Basic

  describe "Info.topics/1" do
    test "topics are parsed at the top level, without a wrapper block" do
      assert [:pets, :system, :typed_payloads] =
               Basic
               |> Info.topics()
               |> Enum.map(& &1.name)
               |> Enum.sort()
    end
  end

  describe "Info.topic/2" do
    test "returns the topic by name" do
      assert {:ok, topic} = Info.topic(Basic, :pets)
      assert topic.segments == ["owners", :owner_id, "pets"]
    end

    test "returns :error for an unknown topic" do
      assert :error = Info.topic(Basic, :nope)
    end

    test "payload fields are parsed and exposed but not enforced" do
      {:ok, topic} = Info.topic(Basic, :pets)
      adopted = Enum.find(topic.messages, &(&1.name == :adopted))

      assert [{:id, :string}, {:name, :string}] = Enum.map(adopted.fields, &{&1.name, &1.type})
    end
  end

  describe "Info.topic!/2" do
    test "raises for an unknown topic and names the known ones" do
      assert_raise ArgumentError, ~r/unknown topic :nope.*:pets/s, fn ->
        Info.topic!(Basic, :nope)
      end
    end

    test "shows each known topic's wire pattern" do
      error = assert_raise ArgumentError, fn -> Info.topic!(Basic, :nope) end
      assert Exception.message(error) =~ ~r/:pets\s+owners:<owner_id>:pets/
    end
  end

  describe "Info.match/2" do
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

  describe "Info.events/2" do
    test "returns declared event names in declaration order" do
      assert [:adopted, :renamed, :rehomed] = Info.events(Basic, :pets)
      assert [:alert] = Info.events(Basic, :system)
    end
  end

  describe "use VerifiedPubSub.Manifest" do
    test "records its Phoenix.PubSub name" do
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

  describe "topic, message, field :description" do
    test "is read from a block or a keyword on a topic, an event and a field" do
      manifest =
        VerifiedPubSub.CompileHelper.compile!("""
        defmodule #{VerifiedPubSub.CompileHelper.unique_module("VPTest.Described")} do
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

          topic :pets, ["pets"] do
            description "Pets."

            message :adopted do
              description "Adopted."
              field :id, :string, description: "The pet."
              field :name, :string
            end

            message :rehomed, description: "Rehomed."
          end
        end
        """)

      topic = Info.topic!(manifest, :pets)
      [adopted, rehomed] = topic.messages

      assert topic.description == "Pets."
      assert adopted.description == "Adopted."
      assert rehomed.description == "Rehomed."
      assert [%{description: "The pet."}, %{description: nil}] = adopted.fields
    end

    test "defaults to nil" do
      assert {:ok, %{description: nil, messages: [%{description: nil} | _]}} =
               Info.topic(Basic, :pets)
    end

    test "must be a string" do
      error =
        VerifiedPubSub.CompileHelper.dsl_error("""
        defmodule #{VerifiedPubSub.CompileHelper.unique_module("VPTest.BadDescription")} do
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

          topic :pets, ["pets"] do
            message :adopted, description: :oops
          end
        end
        """)

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "description"
    end
  end
end
