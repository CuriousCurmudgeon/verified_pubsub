defmodule VerifiedPubSub.HandleMessageParamsTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message

  defmodule Listener do
    use VerifiedPubSub.Subscriber, registry: VerifiedPubSub.TestRegistries.Basic

    # Binds from the topic params, no %Message{} destructuring.
    handle_message :campaigns, %{account_id: acct}, :created, payload, state do
      {:noreply, [{:created, acct, payload.id} | state]}
    end

    # The params-free form still works, in the same module.
    handle_message :campaigns, :updated, payload, state do
      {:noreply, [{:updated, payload.id} | state]}
    end

    ignore_message :campaigns, :deleted
  end

  defmodule Narrowed do
    use VerifiedPubSub.Subscriber, registry: VerifiedPubSub.TestRegistries.Basic

    # A literal narrows to one param value. Order is load-bearing, as with any clauses.
    handle_message :campaigns, %{account_id: "7"}, :created, payload, state do
      {:noreply, [{:seven, payload.id} | state]}
    end

    handle_message :campaigns, %{account_id: acct}, :created, payload, state do
      {:noreply, [{:other, acct, payload.id} | state]}
    end

    ignore_message :campaigns, [:updated, :deleted]
  end

  defp message(event, account_id, payload) do
    %Message{
      registry: VerifiedPubSub.TestRegistries.Basic,
      topic: :campaigns,
      event: event,
      params: %{account_id: account_id},
      payload: payload
    }
  end

  test "a params pattern binds the param" do
    assert {:noreply, [{:created, "42", "c1"}]} =
             Listener.handle_info(message(:created, "42", %{id: "c1"}), [])
  end

  test "the params-free form still works alongside it" do
    assert {:noreply, [{:updated, "c2"}]} =
             Listener.handle_info(message(:updated, "1", %{id: "c2"}), [])
  end

  test "a literal param value narrows the clause" do
    assert {:noreply, [{:seven, "c1"}]} =
             Narrowed.handle_info(message(:created, "7", %{id: "c1"}), [])

    assert {:noreply, [{:other, "8", "c1"}]} =
             Narrowed.handle_info(message(:created, "8", %{id: "c1"}), [])
  end

  test "ignore_message is unchanged" do
    assert {:noreply, []} = Listener.handle_info(message(:deleted, "1", %{id: "c1"}), [])
  end

  defp source(body) do
    """
    defmodule #{unique_module("VPTest.Params")} do
      use VerifiedPubSub.Subscriber, registry: VerifiedPubSub.TestRegistries.Basic
      #{body}
    end
    """
  end

  test "a param the topic does not declare is a compile error" do
    error =
      compile_error(
        source("""
        handle_message :campaigns, %{acount_id: a}, :created, p, s do
          {:noreply, [a, p | s]}
        end

        ignore_message :campaigns, [:updated, :deleted]
        """)
      )

    assert %CompileError{} = error
    message = Exception.message(error)
    assert message =~ ":acount_id"
    assert message =~ "could never match"
  end

  test "matching a subset of the params is allowed" do
    # :shapes takes only :owner_id, but the point is that a partial match is legal where
    # a partial broadcast would not be. Use a topic with one param and match nothing.
    assert is_atom(
             compile!(
               source("""
               handle_message :campaigns, %{}, :created, p, s do
                 {:noreply, [p | s]}
               end

               ignore_message :campaigns, [:updated, :deleted]
               """)
             )
           )
  end

  test "a params pattern plus a whole-message pattern is a compile error" do
    error =
      compile_error(
        source("""
        handle_message :campaigns, %{account_id: a}, :created,
                       %VerifiedPubSub.Message{payload: p}, s do
          {:noreply, [a, p | s]}
        end

        ignore_message :campaigns, [:updated, :deleted]
        """)
      )

    assert %CompileError{} = error
    assert Exception.message(error) =~ "cannot also be a whole"
  end

  test "coverage checking is unaffected" do
    error =
      compile_error(
        source("""
        handle_message :campaigns, %{account_id: a}, :created, p, s do
          {:noreply, [a, p | s]}
        end
        """)
      )

    assert %CompileError{} = error
    assert Exception.message(error) =~ "does not account for"
  end

  test "a runtime-built params pattern is left alone" do
    # Not a literal map, so there are no keys to check. It must still compile.
    assert is_atom(
             compile!(
               source("""
               handle_message :campaigns, params, :created, p, s do
                 {:noreply, [params, p | s]}
               end

               ignore_message :campaigns, [:updated, :deleted]
               """)
             )
           )
  end

  describe "a payload pattern that can never match" do
    test "an undeclared key is a compile error" do
      error =
        compile_error(
          source("""
          handle_message :campaigns, :created, %{nope: x}, s do
            {:noreply, [x | s]}
          end

          ignore_message :campaigns, [:updated, :deleted]
          """)
        )

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ ":nope"
      assert message =~ "could never match"
    end

    test "params in the payload slot are named as such, with the right order shown" do
      # The likely mistake once handle_message/6 exists: writing broadcast!'s argument
      # order but omitting the params slot, so the params land in the payload slot.
      error =
        compile_error(
          source("""
          handle_message :campaigns, :created, %{account_id: a}, s do
            {:noreply, [a | s]}
          end

          ignore_message :campaigns, [:updated, :deleted]
          """)
        )

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "is a param"
      assert message =~ "handle_message :campaigns, %{account_id: ...}, :created"
    end

    test "a subset of the declared fields is fine" do
      assert is_atom(
               compile!(
                 source("""
                 handle_message :campaigns, :created, %{id: id}, s do
                   {:noreply, [id | s]}
                 end

                 ignore_message :campaigns, [:updated, :deleted]
                 """)
               )
             )
    end

    test "an optional field may be matched" do
      assert is_atom(
               compile!(
                 source("""
                 handle_message :campaigns, :created, %{name: n}, s do
                   {:noreply, [n | s]}
                 end

                 ignore_message :campaigns, [:updated, :deleted]
                 """)
               )
             )
    end
  end
end
