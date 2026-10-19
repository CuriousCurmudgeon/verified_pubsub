defmodule VerifiedPubSub.HandleMessageParamsTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Message

  import VerifiedPubSub.CompileHelper

  defmodule Listener do
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

    # Binds from the topic params, no %Message{} destructuring.
    handle_message :pets, %{owner_id: owner}, :adopted, payload, state do
      {:noreply, [{:adopted, owner, payload.id} | state]}
    end

    # The params-free form still works, in the same module.
    handle_message :pets, :renamed, payload, state do
      {:noreply, [{:renamed, payload.id} | state]}
    end

    ignore_message :pets, :rehomed
  end

  defmodule Narrowed do
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

    # A literal narrows to one param value. Order is load-bearing, as with any clauses.
    handle_message :pets, %{owner_id: "7"}, :adopted, payload, state do
      {:noreply, [{:seven, payload.id} | state]}
    end

    handle_message :pets, %{owner_id: owner}, :adopted, payload, state do
      {:noreply, [{:other, owner, payload.id} | state]}
    end

    ignore_message :pets, [:renamed, :rehomed]
  end

  defp message(event, owner_id, payload) do
    %Message{
      manifest: VerifiedPubSub.TestManifests.Basic,
      topic: :pets,
      event: event,
      params: %{owner_id: owner_id},
      payload: payload
    }
  end

  defp source(body) do
    """
    defmodule #{unique_module("VPTest.Params")} do
      use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic
      #{body}
    end
    """
  end

  describe "handle_message/6" do
    test "a params pattern binds the param" do
      assert {:noreply, [{:adopted, "42", "c1"}]} =
               Listener.handle_info(message(:adopted, "42", %{id: "c1"}), [])
    end

    test "a literal param value narrows the clause" do
      assert {:noreply, [{:seven, "c1"}]} =
               Narrowed.handle_info(message(:adopted, "7", %{id: "c1"}), [])

      assert {:noreply, [{:other, "8", "c1"}]} =
               Narrowed.handle_info(message(:adopted, "8", %{id: "c1"}), [])
    end

    test "a param the topic does not declare is a compile error" do
      error =
        compile_error(
          source("""
          handle_message :pets, %{acount_id: a}, :adopted, p, s do
            {:noreply, [a, p | s]}
          end

          ignore_message :pets, [:renamed, :rehomed]
          """)
        )

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ ":acount_id"
      assert message =~ "could never match"
      assert message =~ ~r{:pets is declared at test/support/manifests\.ex:\d+\.}
    end

    test "matching a subset of the params is allowed" do
      # A partial match is legal where a partial broadcast would not be. :pets takes only
      # :owner_id, so the partial match here is matching nothing.
      assert is_atom(
               compile!(
                 source("""
                 handle_message :pets, %{}, :adopted, p, s do
                   {:noreply, [p | s]}
                 end

                 ignore_message :pets, [:renamed, :rehomed]
                 """)
               )
             )
    end

    test "a params pattern plus a whole-message pattern is a compile error" do
      error =
        compile_error(
          source("""
          handle_message :pets, %{owner_id: a}, :adopted,
                         %VerifiedPubSub.Message{payload: p}, s do
            {:noreply, [a, p | s]}
          end

          ignore_message :pets, [:renamed, :rehomed]
          """)
        )

      assert %CompileError{} = error
      assert Exception.message(error) =~ "cannot also be a whole"
    end

    test "a runtime-built params pattern is left alone" do
      # Not a literal map, so there are no keys to check. It must still compile.
      assert is_atom(
               compile!(
                 source("""
                 handle_message :pets, params, :adopted, p, s do
                   {:noreply, [params, p | s]}
                 end

                 ignore_message :pets, [:renamed, :rehomed]
                 """)
               )
             )
    end
  end

  describe "handle_message/5" do
    test "works alongside handle_message/6 in one module" do
      assert {:noreply, [{:renamed, "c2"}]} =
               Listener.handle_info(message(:renamed, "1", %{id: "c2"}), [])
    end

    test "an undeclared payload key is a compile error" do
      error =
        compile_error(
          source("""
          handle_message :pets, :adopted, %{nope: x}, s do
            {:noreply, [x | s]}
          end

          ignore_message :pets, [:renamed, :rehomed]
          """)
        )

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ ":nope"
      assert message =~ "could never match"
      assert message =~ ~r{:adopted on :pets is declared at test/support/manifests\.ex:\d+\.}
    end

    test "params in the payload slot are named as such, with the right order shown" do
      # The likely mistake once handle_message/6 exists: writing broadcast!'s argument
      # order but omitting the params slot, so the params land in the payload slot.
      error =
        compile_error(
          source("""
          handle_message :pets, :adopted, %{owner_id: a}, s do
            {:noreply, [a | s]}
          end

          ignore_message :pets, [:renamed, :rehomed]
          """)
        )

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "is a param"
      assert message =~ "handle_message :pets, %{owner_id: ...}, :adopted"
    end

    test "a subset of the declared fields is fine" do
      assert is_atom(
               compile!(
                 source("""
                 handle_message :pets, :adopted, %{id: id}, s do
                   {:noreply, [id | s]}
                 end

                 ignore_message :pets, [:renamed, :rehomed]
                 """)
               )
             )
    end

    test "an optional field may be matched" do
      assert is_atom(
               compile!(
                 source("""
                 handle_message :pets, :adopted, %{name: n}, s do
                   {:noreply, [n | s]}
                 end

                 ignore_message :pets, [:renamed, :rehomed]
                 """)
               )
             )
    end
  end

  describe "ignore_message/2" do
    test "works alongside handle_message/6" do
      assert {:noreply, []} = Listener.handle_info(message(:rehomed, "1", %{id: "c1"}), [])
    end
  end

  describe "use VerifiedPubSub.Subscriber" do
    test "coverage checking is unaffected by handle_message/6" do
      error =
        compile_error(
          source("""
          handle_message :pets, %{owner_id: a}, :adopted, p, s do
            {:noreply, [a, p | s]}
          end
          """)
        )

      assert %CompileError{} = error
      assert Exception.message(error) =~ "does not account for"
    end
  end
end
