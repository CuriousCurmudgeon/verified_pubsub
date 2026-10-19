defmodule VerifiedPubSub.TopicTest do
  use ExUnit.Case, async: true

  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TopicError

  doctest VerifiedPubSub.Topic

  # A param value that contains the separator would let a caller build a wire topic other
  # than the one that the call site names. ["a", :x] and ["a", :x, "b"] are disjoint as
  # declared. But x = "1:b" makes the first topic build what the second topic builds.
  describe "a param value that contains the separator" do
    test "raises, and builds no wire topic for another topic" do
      error =
        assert_raise TopicError, fn ->
          subscribe(:pets, %{owner_id: "1:pets"})
        end

      message = Exception.message(error)
      assert message =~ ":owner_id"
      assert message =~ "1:pets"
      assert message =~ ":pets (owners:<owner_id>:pets)"
    end

    test "raises from broadcast too" do
      assert_raise TopicError, fn ->
        broadcast!(:pets, %{owner_id: "1:x"}, :adopted, %{id: "c1"})
      end
    end

    test "raises from topic/2" do
      assert_raise TopicError, fn -> topic(:pets, %{owner_id: "a:b"}) end
    end

    test "raises from unsubscribe" do
      assert_raise TopicError, fn -> unsubscribe(:pets, %{owner_id: "a:b"}) end
    end

    test "leaves the process with no subscription when the value fails the check" do
      # If the guard ran after Phoenix.PubSub.subscribe, the process would stay subscribed
      # to the false wire topic.
      assert_raise TopicError, fn -> subscribe(:pets, %{owner_id: "9:pets"}) end
      assert :ok = broadcast!(:pets, %{owner_id: "9"}, :adopted, %{id: "leak"})
      refute_receive %Message{payload: %{id: "leak"}}, 50
    end
  end

  describe "an empty param value" do
    test "raises, because it makes a segment empty" do
      error = assert_raise TopicError, fn -> subscribe(:pets, %{owner_id: ""}) end
      assert Exception.message(error) =~ "empty"
    end
  end

  describe "a valid param value" do
    test "an integer becomes a string" do
      assert topic(:pets, %{owner_id: 7}) == "owners:7:pets"
    end

    test "a UUID is fine" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      assert topic(:pets, %{owner_id: uuid}) == "owners:#{uuid}:pets"
    end

    test "a topic that takes no params still builds its wire topic" do
      assert topic(:system, %{}) == "system"
    end
  end

  describe "two topics where one extends the other" do
    defmodule Adjacent do
      use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Adjacent

      def listen_long(x), do: subscribe(:long, %{x: x})
      def ping_short(x, tag), do: broadcast!(:short, %{x: x}, :ping, %{tag: tag})
    end

    test "a separator in a param value cannot reach the subscribers of the other topic" do
      # :short is ["adj", :x] and :long is ["adj", :x, "b"], so they are disjoint as declared.
      # Before the guard, x = "1:b" on :short built "adj:1:b". :long builds the same wire
      # topic from x = "1", so a :short broadcast reached :long subscribers as an unhandled
      # event.
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
