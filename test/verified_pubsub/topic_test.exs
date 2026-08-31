defmodule VerifiedPubSub.TopicTest do
  use ExUnit.Case, async: true

  use VerifiedPubSub, registry: VerifiedPubSub.TestRegistries.Basic

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TopicError

  # Interpolating a param value that contains the segment separator would let a caller
  # build a topic other than the one the call site names. "a:%{x}" and "a:%{x}:b" are
  # disjoint as declared, but x = "1:b" makes the first build what the second builds.
  describe "a param value carrying the separator" do
    test "raises rather than forging a topic" do
      error =
        assert_raise TopicError, fn ->
          subscribe(:campaigns, %{account_id: "1:campaigns"})
        end

      message = Exception.message(error)
      assert message =~ ":account_id"
      assert message =~ "1:campaigns"
      assert message =~ ":"
    end

    test "raises from broadcast too" do
      assert_raise TopicError, fn ->
        broadcast!(:campaigns, %{account_id: "1:x"}, :created, %{id: "c1"})
      end
    end

    test "raises from topic/2" do
      assert_raise TopicError, fn -> topic(:campaigns, %{account_id: "a:b"}) end
    end

    test "raises from unsubscribe" do
      assert_raise TopicError, fn -> unsubscribe(:campaigns, %{account_id: "a:b"}) end
    end

    test "nothing is subscribed when the value is rejected" do
      # If the guard ran after Phoenix.PubSub.subscribe, the process would be left
      # subscribed to the forged topic.
      assert_raise TopicError, fn -> subscribe(:campaigns, %{account_id: "9:campaigns"}) end
      assert :ok = broadcast!(:campaigns, %{account_id: "9"}, :created, %{id: "leak"})
      refute_receive %Message{payload: %{id: "leak"}}, 50
    end
  end

  describe "an empty param value" do
    test "raises, since it collapses a segment" do
      error = assert_raise TopicError, fn -> subscribe(:campaigns, %{account_id: ""}) end
      assert Exception.message(error) =~ "empty"
    end
  end

  describe "values that are fine" do
    test "an integer is stringified" do
      assert topic(:campaigns, %{account_id: 7}) == "accounts:7:campaigns"
    end

    test "a UUID is fine" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert topic(:campaigns, %{account_id: uuid}) == "accounts:#{uuid}:campaigns"
    end

    test "a param-free topic is unaffected" do
      assert topic(:system, %{}) == "system"
    end
  end

  describe "cross-topic delivery" do
    defmodule Adjacent do
      use VerifiedPubSub, registry: VerifiedPubSub.TestRegistries.Adjacent

      def listen_long(x), do: subscribe(:long, %{x: x})
      def ping_short(x, tag), do: broadcast!(:short, %{x: x}, :ping, %{tag: tag})
    end

    test "a separator in a param cannot reach another topic's subscribers" do
      # :short is "adj:%{x}" and :long is "adj:%{x}:b" -- disjoint as declared. Before the
      # guard, x = "1:b" on :short built "adj:1:b", exactly what :long builds from x = "1",
      # so a :short broadcast was delivered to :long subscribers as an unhandled event.
      assert :ok = Adjacent.listen_long("1")

      assert_raise TopicError, fn -> Adjacent.ping_short("1:b", "forged") end

      refute_receive %Message{payload: %{tag: "forged"}}, 50
    end

    test "the two topics still work normally" do
      assert :ok = Adjacent.listen_long("2")
      assert :ok = Adjacent.ping_short("2", "fine")
      refute_receive %Message{payload: %{tag: "fine"}}, 50
    end
  end
end
