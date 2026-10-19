defmodule VerifiedPubSub.ApiTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Message

  import VerifiedPubSub.CompileHelper

  defmodule Broadcaster do
    use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

    def sub(id), do: subscribe(:pets, %{owner_id: id})
    def unsub(id), do: unsubscribe(:pets, %{owner_id: id})
    def topic_for(id), do: topic(:pets, %{owner_id: id})

    def adopted(id, payload) do
      broadcast!(:pets, %{owner_id: id}, :adopted, payload)
    end

    def adopted_plain(id, payload) do
      broadcast(:pets, %{owner_id: id}, :adopted, payload)
    end

    def adopted_from(from, id, payload) do
      broadcast_from!(from, :pets, %{owner_id: id}, :adopted, payload)
    end

    def alert(payload), do: broadcast!(:system, %{}, :alert, payload)
    def sub_system, do: subscribe(:system)

    # This function builds the params at runtime, so the map is not a literal when the
    # macro expands.
    def adopted_dynamic(id, payload) do
      params = Map.new([{:owner_id, id}])
      broadcast!(:pets, params, :adopted, payload)
    end
  end

  setup do
    %{owner_id: unique_owner_id()}
  end

  defp source(body) do
    """
    defmodule #{unique_module("VPTest.Api")} do
      use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
      def go, do: #{body}
    end
    """
  end

  describe "import VerifiedPubSub.Api" do
    test "a macro without a manifest in scope is a compile error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.NoReg")} do
          import VerifiedPubSub.Api
          def go, do: broadcast!(:pets, %{owner_id: "1"}, :adopted, %{})
        end
        """)

      assert %CompileError{} = error
      assert Exception.message(error) =~ "no manifest is in scope"
    end
  end

  describe "subscribe/1" do
    test "a topic that takes no params needs no params argument" do
      token = unique_owner_id()
      assert :ok = Broadcaster.sub_system()
      assert :ok = Broadcaster.alert(%{text: token})

      assert_receive %Message{
        topic: :system,
        event: :alert,
        params: %{},
        payload: %{text: ^token}
      }
    end
  end

  describe "unsubscribe/2" do
    test "stops delivery", %{owner_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.unsub(id)
      assert :ok = Broadcaster.adopted(id, %{id: "c1"})

      refute_receive %Message{}, 50
    end
  end

  describe "topic/2" do
    test "puts the params into the wire topic", %{owner_id: id} do
      assert Broadcaster.topic_for(id) == "owners:#{id}:pets"
    end
  end

  describe "broadcast/4" do
    test "returns :ok", %{owner_id: id} do
      assert :ok = Broadcaster.adopted_plain(id, %{id: "c1"})
    end
  end

  describe "broadcast!/4" do
    test "delivers a message to a subscribed process", %{owner_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.adopted(id, %{id: "c1"})

      assert_receive %Message{topic: :pets, event: :adopted, payload: %{id: "c1"}}
    end

    test "the message has the same shape that the generated functions use", %{owner_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.adopted(id, %{id: "c1"})

      assert_receive %Message{
        manifest: VerifiedPubSub.TestManifests.Basic,
        topic: :pets,
        event: :adopted,
        params: %{owner_id: ^id},
        payload: %{id: "c1"}
      }
    end

    test "accepts params built at runtime", %{owner_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.adopted_dynamic(id, %{id: "c1"})

      assert_receive %Message{params: %{owner_id: ^id}}
    end

    test "params and an event in the wrong order are a compile error, not a runtime error" do
      # Before 0.1, the order was (topic, event, params, payload). In that order, the
      # params map goes in the event slot, where the macro cannot verify a value that is
      # not an atom. So the old order stops the build. It never reaches Map.fetch!/2 on
      # an atom at runtime.
      error = compile_error(source(~s|broadcast!(:pets, :adopted, %{owner_id: "1"}, %{})|))

      assert %CompileError{} = error
      assert Exception.message(error) =~ "expected a literal atom for event"
    end

    test "an unknown topic is a compile error that lists the declared topics" do
      error = compile_error(source(~s|broadcast!(:nope, %{}, :adopted, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "unknown topic :nope"

      assert message =~
               ~r{:pets\s+owners:<owner_id>:pets\s+test/support/manifests\.ex:\d+}

      assert message =~ ~r{:system\s+system\s+test/support/manifests\.ex:\d+}
    end

    test "a misspelled event is a compile error that lists the declared events" do
      error =
        compile_error(source(~s|broadcast!(:pets, %{owner_id: "1"}, :adoptd, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "unknown event :adoptd"
      assert message =~ "on topic :pets (owners:<owner_id>:pets)"
      assert message =~ ":adopted"
      assert message =~ ~r{:pets is declared at test/support/manifests\.ex:\d+\.}
    end

    test "an event of another topic is a compile error that names the topic that declares it" do
      error =
        compile_error(source(~s|broadcast!(:pets, %{owner_id: "1"}, :alert, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "unknown event :alert"
      assert message =~ "declared on [:system]"
    end

    test "a literal params map without a required key is a compile error" do
      error = compile_error(source(~s|broadcast!(:pets, %{}, :adopted, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "owner_id"
      assert message =~ ":pets is owners:<owner_id>:pets, so it takes"
      assert message =~ ~r{:pets is declared at test/support/manifests\.ex:\d+\.}
    end

    test "a literal params map with an unexpected key is a compile error" do
      error =
        compile_error(source(~s|broadcast!(:pets, %{owner_id: "1", extra: 2}, :adopted, %{})|))

      assert %CompileError{} = error
      assert Exception.message(error) =~ "extra"
    end

    test "a literal params map with a misspelled key names that key and the declared key" do
      error =
        compile_error(source(~s|broadcast!(:pets, %{acount_id: "1"}, :adopted, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "acount_id"
      assert message =~ "owner_id"
    end

    test "params built at runtime get no compile check and are not an error" do
      assert is_atom(
               compile!(
                 source(~s|broadcast!(:pets, Map.new([{:owner_id, "1"}]), :adopted, %{id: "x"})|)
               )
             )
    end

    test "a topic that takes no params accepts an empty literal map" do
      assert is_atom(compile!(source(~s|broadcast!(:system, %{}, :alert, %{text: "x"})|)))
    end

    test "a topic that is not a literal is a compile error" do
      error = compile_error(source(~s|broadcast!(var!(t), %{owner_id: "1"}, :adopted, %{})|))

      assert %CompileError{} = error
      assert Exception.message(error) =~ "literal atom"
    end
  end

  describe "broadcast_from!/5" do
    test "excludes the sender", %{owner_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.adopted_from(self(), id, %{id: "c1"})

      refute_receive %Message{}, 50
    end
  end

  describe "use VerifiedPubSub.Subscriber" do
    test "a subscriber can broadcast with its manifest and no second use" do
      # `use VerifiedPubSub.Subscriber` already sets the manifest attribute. So a
      # LiveView that subscribes can broadcast when it imports only the macros.
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.SubAndBroadcast")} do
                 use VerifiedPubSub.Subscriber,
                   manifest: VerifiedPubSub.TestManifests.Basic

                 import VerifiedPubSub.Api

                 def go(id) do
                   broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "x"})
                 end

                 handle_message :pets, :adopted, p, s do
                   {:noreply, {p, s}}
                 end

                 ignore_message :pets, :renamed
                 ignore_message :pets, :rehomed
               end
               """)
             )
    end

    test "a subscriber can use the manifest that the module is already bound to" do
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.SameManifest")} do
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

                 use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

                 handle_message :pets, :adopted, p, s do
                   {:noreply, {p, s}}
                 end

                 ignore_message :pets, [:renamed, :rehomed]
               end
               """)
             )
    end

    test "a subscriber cannot rebind a module to another manifest" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.RebindSubscriber")} do
          use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

          use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.CollideA
        end
        """)

      assert %ArgumentError{} = error

      assert Exception.message(error) =~
               "already bound to manifest VerifiedPubSub.TestManifests.Basic"

      assert Exception.message(error) =~ "cannot also bind VerifiedPubSub.TestManifests.CollideA"
    end

    test "a module cannot use VerifiedPubSub.Subscriber twice" do
      for second <- [VerifiedPubSub.TestManifests.Basic, VerifiedPubSub.TestManifests.CollideA] do
        error =
          compile_error("""
          defmodule #{unique_module("VPTest.TwoSubscribers")} do
            use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic
            use VerifiedPubSub.Subscriber, manifest: #{inspect(second)}
          end
          """)

        assert %ArgumentError{} = error
        assert Exception.message(error) =~ "already uses VerifiedPubSub.Subscriber"
      end
    end
  end
end
