defmodule VerifiedPubSub.BroadcastTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestRegistries.Basic

  setup do
    %{account_id: unique_account_id()}
  end

  test "topic_* interpolates params into the wire pattern" do
    assert Basic.topic_campaigns(%{account_id: "7"}) == "accounts:7:campaigns"
  end

  test "topic_* stringifies non-binary params" do
    assert Basic.topic_campaigns(%{account_id: 7}) == "accounts:7:campaigns"
  end

  test "a param-free topic takes no params argument" do
    assert Basic.topic_system() == "system"
  end

  test "broadcast_*! delivers a Message to subscribers of that topic", %{account_id: id} do
    assert :ok = Basic.subscribe_campaigns(%{account_id: id})
    assert :ok = Basic.broadcast_campaigns_created!(%{account_id: id}, %{id: "c1"})

    assert_receive %Message{
      registry: Basic,
      topic: :campaigns,
      event: :created,
      params: %{account_id: ^id},
      payload: %{id: "c1"}
    }
  end

  test "broadcasts are scoped by param value", %{account_id: id} do
    assert :ok = Basic.subscribe_campaigns(%{account_id: id})
    assert :ok = Basic.broadcast_campaigns_created!(%{account_id: id <> "other"}, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "unsubscribe_* stops delivery", %{account_id: id} do
    assert :ok = Basic.subscribe_campaigns(%{account_id: id})
    assert :ok = Basic.unsubscribe_campaigns(%{account_id: id})
    assert :ok = Basic.broadcast_campaigns_created!(%{account_id: id}, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "the non-bang broadcast returns :ok" do
    assert :ok = Basic.broadcast_campaigns_created(%{account_id: "7"}, %{id: "c1"})
  end

  test "a param-free topic broadcasts with only a payload" do
    assert :ok = Basic.subscribe_system()
    assert :ok = Basic.broadcast_system_alert!(%{text: "hi"})

    assert_receive %Message{topic: :system, event: :alert, params: %{}}
  end

  test "a wrong param key in a literal map is flagged at compile time" do
    # The generated head destructures the topic's params, so Elixir's type inference
    # catches a literal map with the wrong keys without any Dialyzer run.
    {_result, diagnostics} =
      Code.with_diagnostics(fn ->
        Code.compile_string("""
        defmodule #{unique_module("VPTest.BadParams")} do
          def go do
            VerifiedPubSub.TestRegistries.Basic.broadcast_campaigns_created!(
              %{wrong: "7"},
              %{id: "c1"}
            )
          end
        end
        """)
      end)

    assert diagnostic =
             Enum.find(diagnostics, &(&1.message =~ "broadcast_campaigns_created!")),
           "expected a diagnostic for the wrong param key, got: #{inspect(diagnostics)}"

    assert diagnostic.message =~ "incompatible types"
    assert diagnostic.message =~ "account_id"
  end

  test "a missing param key raises FunctionClauseError at runtime" do
    # Built so the type checker cannot see the keys, which is the dynamic case the
    # compile-time check above cannot cover.
    params = Map.new([{String.to_atom("wrong"), "7"}])

    assert_raise FunctionClauseError, fn ->
      Basic.broadcast_campaigns_created!(params, %{id: "c1"})
    end
  end

  test "broadcast_*_from! excludes the sender", %{account_id: id} do
    assert :ok = Basic.subscribe_campaigns(%{account_id: id})

    assert :ok =
             Basic.broadcast_campaigns_created_from!(self(), %{account_id: id}, %{id: "c1"})

    refute_receive %Message{}, 50
  end

  test "broadcast_*_from! still delivers to other subscribers", %{account_id: id} do
    test_pid = self()

    other =
      spawn_link(fn ->
        Basic.subscribe_campaigns(%{account_id: id})
        send(test_pid, :ready)
        receive do: (%Message{payload: p} -> send(test_pid, {:other_got, p}))
      end)

    assert_receive :ready
    Basic.subscribe_campaigns(%{account_id: id})
    Basic.broadcast_campaigns_created_from!(self(), %{account_id: id}, %{id: "c1"})

    assert_receive {:other_got, %{id: "c1"}}
    refute_receive %Message{}, 50
    Process.exit(other, :kill)
  end

  test "the non-bang from variant returns :ok", %{account_id: id} do
    assert :ok = Basic.broadcast_campaigns_created_from(self(), %{account_id: id}, %{id: "c1"})
  end

  test "a param-free topic's from variant takes only from and payload" do
    assert :ok = Basic.subscribe_system()
    assert :ok = Basic.broadcast_system_alert_from!(self(), %{text: "hi"})

    refute_receive %Message{topic: :system}, 50
  end

  test "the from variant carries the same message shape as the base variant", %{
    account_id: id
  } do
    test_pid = self()

    other =
      spawn_link(fn ->
        Basic.subscribe_campaigns(%{account_id: id})
        send(test_pid, :ready)
        receive do: (m -> send(test_pid, {:got, m}))
      end)

    assert_receive :ready
    Basic.broadcast_campaigns_created_from!(self(), %{account_id: id}, %{id: "c1"})

    assert_receive {:got,
                    %Message{
                      registry: Basic,
                      topic: :campaigns,
                      event: :created,
                      params: %{account_id: ^id},
                      payload: %{id: "c1"}
                    }}

    Process.exit(other, :kill)
  end

  test "no function is generated for an undeclared event" do
    refute function_exported?(Basic, :broadcast_campaigns_exploded!, 2)
  end

  test "broadcasting an undeclared event is flagged at compile time" do
    # Elixir reports an undefined *remote* function as a warning, not an error. It
    # becomes a hard failure under `mix compile --warnings-as-errors`, which is the
    # documented way to enforce this guarantee in CI.
    {_result, diagnostics} =
      Code.with_diagnostics(fn ->
        Code.compile_string("""
        defmodule #{unique_module("VPTest.BadBroadcast")} do
          def go do
            VerifiedPubSub.TestRegistries.Basic.broadcast_campaigns_exploded!(
              %{account_id: "7"},
              %{}
            )
          end
        end
        """)
      end)

    assert diagnostic =
             Enum.find(diagnostics, &(&1.message =~ "broadcast_campaigns_exploded!")),
           "expected a diagnostic naming the undeclared event, got: #{inspect(diagnostics)}"

    assert diagnostic.severity == :warning
    assert diagnostic.message =~ "is undefined or private"

    # The message lists the valid events on that topic, which is what makes the
    # undefined-function approach usable without a macro.
    assert diagnostic.message =~ "broadcast_campaigns_created!/2"
  end
end
