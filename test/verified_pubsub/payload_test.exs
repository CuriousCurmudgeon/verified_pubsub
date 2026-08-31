defmodule VerifiedPubSub.PayloadTest do
  use ExUnit.Case, async: true

  use VerifiedPubSub, registry: VerifiedPubSub.TestRegistries.Basic

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.PayloadError
  alias VerifiedPubSub.TestRegistries.Basic
  alias VerifiedPubSub.TestStructs.Point

  setup do
    %{owner_id: unique_account_id()}
  end

  defp valid_typed do
    %{
      name: "n",
      count: 1,
      ratio: 1.5,
      flag: true,
      kind: :thing,
      meta: %{},
      tags: ["a", "b"],
      anything: {:whatever, 1}
    }
  end

  describe "a valid payload" do
    test "passes and is delivered unchanged", %{owner_id: id} do
      assert :ok = subscribe(:shapes, %{owner_id: id})
      payload = valid_typed()
      assert :ok = broadcast!(:shapes, :typed, %{owner_id: id}, payload)

      assert_receive %Message{event: :typed, payload: ^payload}
    end

    test "an optional field may be omitted", %{owner_id: id} do
      assert :ok = broadcast!(:shapes, :typed, %{owner_id: id}, valid_typed())
    end

    test "an optional field may be supplied", %{owner_id: id} do
      payload = Map.put(valid_typed(), :note, "hi")
      assert :ok = broadcast!(:shapes, :typed, %{owner_id: id}, payload)
    end

    test "a struct field accepts the declared struct", %{owner_id: id} do
      assert :ok =
               broadcast!(:shapes, :structured, %{owner_id: id}, %{point: %Point{x: 1, y: 2}})
    end

    test "an :any field accepts anything", %{owner_id: id} do
      payload = Map.put(valid_typed(), :anything, self())
      assert :ok = broadcast!(:shapes, :typed, %{owner_id: id}, payload)
    end
  end

  describe "shape violations" do
    test "a missing required key raises and names it", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :bare, %{owner_id: id}, %{})
        end

      message = Exception.message(error)
      assert message =~ "missing required key: :id"
      assert message =~ ":shapes"
      assert message =~ ":bare"
    end

    test "an unexpected key raises and names it", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :bare, %{owner_id: id}, %{id: "x", nope: 1})
        end

      assert Exception.message(error) =~ "unexpected key: :nope"
    end

    test "a wrong type raises, naming the field, declared type and value", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :bare, %{owner_id: id}, %{id: 42})
        end

      message = Exception.message(error)
      assert message =~ ":id is declared as :string"
      assert message =~ "got: 42"
    end

    test "every problem is reported at once, not just the first", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :typed, %{owner_id: id}, %{count: "not an int", nope: 1})
        end

      message = Exception.message(error)
      assert message =~ "missing required keys:"
      assert message =~ "unexpected key: :nope"
      assert message =~ ":count is declared as :integer"
    end

    test "a {:list, :string} rejects a list with a bad element", %{owner_id: id} do
      payload = Map.put(valid_typed(), :tags, ["ok", 1])

      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :typed, %{owner_id: id}, payload)
        end

      assert Exception.message(error) =~ "{:list, :string}"
    end

    test "a struct field rejects a different struct", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :structured, %{owner_id: id}, %{
            point: %Message{
              registry: Basic,
              topic: :a,
              event: :b
            }
          })
        end

      assert Exception.message(error) =~ "VerifiedPubSub.TestStructs.Point"
    end

    test "a non-map payload raises", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :bare, %{owner_id: id}, "not a map")
        end

      assert Exception.message(error) =~ "must be a map of the declared fields"
    end

    test "a struct payload explains how to carry it in a field", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, :bare, %{owner_id: id}, %Point{x: 1, y: 2})
        end

      message = Exception.message(error)
      assert message =~ "is a VerifiedPubSub.TestStructs.Point struct"
      assert message =~ "Declare a field to carry it"
    end

    test "nothing is broadcast when validation fails", %{owner_id: id} do
      assert :ok = subscribe(:shapes, %{owner_id: id})

      assert_raise PayloadError, fn ->
        broadcast!(:shapes, :bare, %{owner_id: id}, %{})
      end

      refute_receive %Message{topic: :shapes}, 50
    end

    test "the non-bang broadcast raises too", %{owner_id: id} do
      # A shape violation is a bug, not a transport failure, so it is not reported
      # through the {:error, _} channel that a caller might shrug off.
      assert_raise PayloadError, fn ->
        broadcast(:shapes, :bare, %{owner_id: id}, %{})
      end
    end

    test "the from variants validate as well", %{owner_id: id} do
      assert_raise PayloadError, fn ->
        broadcast_from!(self(), :shapes, :bare, %{owner_id: id}, %{})
      end

      assert_raise PayloadError, fn ->
        broadcast_from(self(), :shapes, :bare, %{owner_id: id}, %{})
      end
    end
  end

  describe "field type declarations" do
    test "an unknown type is a compile error listing the valid ones" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.BadType")} do
          use VerifiedPubSub.Registry, pubsub: VerifiedPubSub.TestPubSub

          topic :t, "t" do
            message :e do
              field :id, :strng
            end
          end
        end
        """)

      assert %Spark.Error.DslError{} = error
      message = Exception.message(error)
      assert message =~ "invalid type :strng"
      assert message =~ ":string"
    end

    test "a bad inner type in a list is a compile error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.BadInner")} do
          use VerifiedPubSub.Registry, pubsub: VerifiedPubSub.TestPubSub

          topic :t, "t" do
            message :e do
              field :tags, {:list, :strng}
            end
          end
        end
        """)

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "invalid type"
    end

    test "a struct module is accepted without being loaded" do
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.StructType")} do
                 use VerifiedPubSub.Registry, pubsub: VerifiedPubSub.TestPubSub

                 topic :t, "t" do
                   message :e do
                     field :thing, SomeApp.NotCompiledYet
                   end
                 end
               end
               """)
             )
    end
  end

  describe "introspection" do
    test "Info.fields/3 exposes the declared fields" do
      assert [%{name: :id, type: :string, required: true}] =
               VerifiedPubSub.Info.fields(Basic, :shapes, :bare)
    end

    test "Info.fields/3 reports required: false" do
      note = Enum.find(VerifiedPubSub.Info.fields(Basic, :shapes, :typed), &(&1.name == :note))
      assert note.required == false
    end

    test "Info.fields/3 raises for an unknown event" do
      assert_raise ArgumentError, ~r/unknown event :nope/, fn ->
        VerifiedPubSub.Info.fields(Basic, :shapes, :nope)
      end
    end
  end
end
