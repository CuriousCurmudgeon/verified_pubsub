defmodule VerifiedPubSub.HandleMessageParamsTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Message

  import VerifiedPubSub.CompileHelper

  defmodule Listener do
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

    # Binds a value from the topic params, and does not destructure %Message{}.
    handle_message :pets, %{owner_id: owner}, :adopted, payload, state do
      {:noreply, [{:adopted, owner, payload.id} | state]}
    end

    # The form without params works in the same module.
    handle_message :pets, :renamed, payload, state do
      {:noreply, [{:renamed, payload.id} | state]}
    end

    ignore_message :pets, :rehomed
  end

  defmodule Narrowed do
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

    # A literal narrows the clause to one param value. As with any clauses, the order
    # matters.
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

    test "a param the topic does not declare is a compile error that names the param and says the pattern can never match" do
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

    test "a pattern can match a subset of the params" do
      # A partial match is valid, but a partial broadcast is not. :pets takes only
      # :owner_id, so the partial match here names no params.
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

    test "a params pattern together with a pattern for the whole message is a compile error" do
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

    test "a params pattern built at runtime gets no check" do
      # It is not a literal map, so it has no keys to check. It must compile.
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

    test "an undeclared payload key is a compile error that names the key and says the pattern can never match" do
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

    test "the error for params in the payload slot names them and shows the correct order" do
      # This mistake is likely once handle_message/6 exists: a call site writes the
      # argument order of broadcast! but leaves out the params slot, so the params go in
      # the payload slot.
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

    test "a pattern can match a subset of the declared fields" do
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
    test "handle_message/6 does not change the coverage check" do
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
