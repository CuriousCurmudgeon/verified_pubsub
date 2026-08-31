defmodule VerifiedPubSub.ApiTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message

  defmodule Broadcaster do
    use VerifiedPubSub, registry: VerifiedPubSub.TestRegistries.Basic

    def sub(id), do: subscribe(:campaigns, %{account_id: id})
    def unsub(id), do: unsubscribe(:campaigns, %{account_id: id})
    def topic_for(id), do: topic(:campaigns, %{account_id: id})

    def created(id, payload) do
      broadcast!(:campaigns, :created, %{account_id: id}, payload)
    end

    def created_plain(id, payload) do
      broadcast(:campaigns, :created, %{account_id: id}, payload)
    end

    def created_from(from, id, payload) do
      broadcast_from!(from, :campaigns, :created, %{account_id: id}, payload)
    end

    def alert(payload), do: broadcast!(:system, :alert, %{}, payload)
    def sub_system, do: subscribe(:system)

    # params built at runtime, so the map is not a literal at expansion time
    def created_dynamic(id, payload) do
      params = Map.new([{:account_id, id}])
      broadcast!(:campaigns, :created, params, payload)
    end
  end

  setup do
    %{account_id: unique_account_id()}
  end

  describe "runtime behaviour" do
    test "topic/2 interpolates params", %{account_id: id} do
      assert Broadcaster.topic_for(id) == "accounts:#{id}:campaigns"
    end

    test "subscribe + broadcast delivers", %{account_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.created(id, %{id: "c1"})

      assert_receive %Message{topic: :campaigns, event: :created, payload: %{id: "c1"}}
    end

    test "the message carries the same shape as the generated functions", %{account_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.created(id, %{id: "c1"})

      assert_receive %Message{
        registry: VerifiedPubSub.TestRegistries.Basic,
        topic: :campaigns,
        event: :created,
        params: %{account_id: ^id},
        payload: %{id: "c1"}
      }
    end

    test "non-bang returns :ok", %{account_id: id} do
      assert :ok = Broadcaster.created_plain(id, %{id: "c1"})
    end

    test "unsubscribe stops delivery", %{account_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.unsub(id)
      assert :ok = Broadcaster.created(id, %{id: "c1"})

      refute_receive %Message{}, 50
    end

    test "broadcast_from excludes the sender", %{account_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.created_from(self(), id, %{id: "c1"})

      refute_receive %Message{}, 50
    end

    test "a param-free topic needs no params argument" do
      token = unique_account_id()
      assert :ok = Broadcaster.sub_system()
      assert :ok = Broadcaster.alert(%{text: token})

      assert_receive %Message{
        topic: :system,
        event: :alert,
        params: %{},
        payload: %{text: ^token}
      }
    end

    test "params built at runtime still work", %{account_id: id} do
      assert :ok = Broadcaster.sub(id)
      assert :ok = Broadcaster.created_dynamic(id, %{id: "c1"})

      assert_receive %Message{params: %{account_id: ^id}}
    end
  end

  describe "compile-time verification" do
    defp source(body) do
      """
      defmodule #{unique_module("VPTest.Api")} do
        use VerifiedPubSub, registry: VerifiedPubSub.TestRegistries.Basic
        def go, do: #{body}
      end
      """
    end

    test "an unknown topic is a hard compile error listing declared topics" do
      error = compile_error(source(~s|broadcast!(:nope, :created, %{}, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "unknown topic :nope"
      assert message =~ ":campaigns"
    end

    test "a typo'd event is a hard compile error listing declared events" do
      error =
        compile_error(source(~s|broadcast!(:campaigns, :creatd, %{account_id: "1"}, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "unknown event :creatd"
      assert message =~ ":created"
    end

    test "an event from another topic is a hard error, and says where it lives" do
      error =
        compile_error(source(~s|broadcast!(:campaigns, :alert, %{account_id: "1"}, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "unknown event :alert"
      assert message =~ "declared on [:system]"
    end

    test "a literal params map missing a required key is a hard compile error" do
      error = compile_error(source(~s|broadcast!(:campaigns, :created, %{}, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "account_id"
      assert message =~ ":campaigns"
    end

    test "a literal params map with an unexpected key is a hard compile error" do
      error =
        compile_error(
          source(~s|broadcast!(:campaigns, :created, %{account_id: "1", extra: 2}, %{})|)
        )

      assert %CompileError{} = error
      assert Exception.message(error) =~ "extra"
    end

    test "a literal params map with a misspelled key names both sides" do
      error =
        compile_error(source(~s|broadcast!(:campaigns, :created, %{acount_id: "1"}, %{})|))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "acount_id"
      assert message =~ "account_id"
    end

    test "params built at runtime skip the compile check and are not an error" do
      assert is_atom(
               compile!(
                 source(
                   ~s|broadcast!(:campaigns, :created, Map.new([{:account_id, "1"}]), %{id: "x"})|
                 )
               )
             )
    end

    test "a param-free topic accepts an empty literal map" do
      assert is_atom(compile!(source(~s|broadcast!(:system, :alert, %{}, %{text: "x"})|)))
    end

    test "a non-literal topic is a hard compile error" do
      error = compile_error(source(~s|broadcast!(var!(t), :created, %{account_id: "1"}, %{})|))

      assert %CompileError{} = error
      assert Exception.message(error) =~ "literal atom"
    end

    test "using the macros without a registry in scope is a compile error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.NoReg")} do
          import VerifiedPubSub.Api
          def go, do: broadcast!(:campaigns, :created, %{account_id: "1"}, %{})
        end
        """)

      assert %CompileError{} = error
      assert Exception.message(error) =~ "no registry is in scope"
    end

    test "a subscriber's registry is reused without a second use" do
      # `use VerifiedPubSub.Subscriber` already sets the registry attribute, so a
      # LiveView that subscribes can broadcast by importing the macros alone.
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.SubAndBroadcast")} do
                 use VerifiedPubSub.Subscriber,
                   registry: VerifiedPubSub.TestRegistries.Basic

                 import VerifiedPubSub.Api

                 def go(id) do
                   broadcast!(:campaigns, :created, %{account_id: id}, %{id: "x"})
                 end

                 handle_message :campaigns, :created, p, s do
                   {:noreply, {p, s}}
                 end

                 ignore_message :campaigns, :updated
                 ignore_message :campaigns, :deleted
               end
               """)
             )
    end
  end
end
