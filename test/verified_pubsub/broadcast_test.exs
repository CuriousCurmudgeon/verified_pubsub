defmodule VerifiedPubsub.BroadcastTest do
  use ExUnit.Case, async: true

  import VerifiedPubsub.CompileHelper

  alias VerifiedPubsub.Message
  alias VerifiedPubsub.TestRegistries.Basic

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

  test "a missing param key raises FunctionClauseError" do
    assert_raise FunctionClauseError, fn ->
      Basic.broadcast_campaigns_created!(%{wrong: "7"}, %{id: "c1"})
    end
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
            VerifiedPubsub.TestRegistries.Basic.broadcast_campaigns_exploded!(
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
