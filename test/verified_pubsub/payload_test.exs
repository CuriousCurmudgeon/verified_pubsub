defmodule VerifiedPubSub.PayloadTest do
  use ExUnit.Case, async: true

  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.Payload
  alias VerifiedPubSub.PayloadError
  alias VerifiedPubSub.TestManifests.Basic
  alias VerifiedPubSub.TestStructs.Point

  setup do
    %{owner_id: unique_account_id()}
  end

  # Passing a payload through a function call hides it from the compile-time literal
  # check, so the runtime validator is what runs. Literal payloads are covered
  # separately below. Identity on purpose: the point is only to defeat AST inspection.
  defp at_runtime(payload), do: payload

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
    # One test through the macro, covering the whole path: validation accepts the
    # payload, `Payload.validate!/4` returns it, and the macro puts that return value on
    # the message. The acceptance cases below go straight at the validator instead --
    # delivery is irrelevant to what they claim, and asserting it there only obscures it.
    test "is delivered unchanged", %{owner_id: id} do
      assert :ok = subscribe(:shapes, %{owner_id: id})
      payload = valid_typed()
      assert :ok = broadcast!(:shapes, %{owner_id: id}, :typed, payload)

      assert_receive %Message{event: :typed, payload: ^payload}
    end
  end

  describe "accepted shapes" do
    # `validate!/4` returns the payload it was given, so asserting on the return value
    # states both halves of the claim: accepted, and unaltered.
    defp accept!(event, payload) do
      assert payload == Payload.validate!(Basic, :shapes, event, payload)
    end

    test "every declared type accepts a matching value, with the optional field omitted" do
      accept!(:typed, valid_typed())
    end

    test "an optional field may be supplied" do
      accept!(:typed, Map.put(valid_typed(), :note, "hi"))
    end

    test "an optional field may be nil" do
      accept!(:typed, Map.put(valid_typed(), :note, nil))
    end

    test "a struct field accepts the declared struct" do
      accept!(:structured, %{point: %Point{x: 1, y: 2}})
    end

    test "an :any field accepts anything" do
      accept!(:typed, Map.put(valid_typed(), :anything, self()))
      accept!(:typed, Map.put(valid_typed(), :anything, nil))
    end
  end

  describe "shape violations" do
    test "a missing required key raises and names it", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, %{owner_id: id}, :bare, at_runtime(%{}))
        end

      message = Exception.message(error)
      assert message =~ "missing required key: :id"
      assert message =~ ":shapes"
      assert message =~ ":bare"
    end

    test "an unexpected key raises and names it", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, %{owner_id: id}, :bare, at_runtime(%{id: "x", nope: 1}))
        end

      assert Exception.message(error) =~ "unexpected key: :nope"
    end

    test "a wrong type raises, naming the field, declared type and value", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, %{owner_id: id}, :bare, at_runtime(%{id: 42}))
        end

      message = Exception.message(error)
      assert message =~ ":id is declared as :string"
      assert message =~ "got: 42"
    end

    test "every problem is reported at once, not just the first", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(
            :shapes,
            %{owner_id: id},
            :typed,
            at_runtime(%{count: "not an int", nope: 1})
          )
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
          broadcast!(:shapes, %{owner_id: id}, :typed, at_runtime(payload))
        end

      assert Exception.message(error) =~ "{:list, :string}"
    end

    test "a struct field rejects a different struct", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, %{owner_id: id}, :structured, %{
            point: %Message{
              manifest: Basic,
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
          broadcast!(:shapes, %{owner_id: id}, :bare, at_runtime("not a map"))
        end

      assert Exception.message(error) =~ "must be a map of the declared fields"
    end

    test "a struct payload explains how to carry it in a field", %{owner_id: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:shapes, %{owner_id: id}, :bare, at_runtime(%Point{x: 1, y: 2}))
        end

      message = Exception.message(error)
      assert message =~ "is a VerifiedPubSub.TestStructs.Point struct"
      assert message =~ "Declare a field to carry it"
    end

    test "nothing is broadcast when validation fails", %{owner_id: id} do
      assert :ok = subscribe(:shapes, %{owner_id: id})

      assert_raise PayloadError, fn ->
        broadcast!(:shapes, %{owner_id: id}, :bare, at_runtime(%{}))
      end

      refute_receive %Message{topic: :shapes}, 50
    end

    test "the non-bang broadcast raises too", %{owner_id: id} do
      # A shape violation is a bug, not a transport failure, so it is not reported
      # through the {:error, _} channel that a caller might shrug off.
      assert_raise PayloadError, fn ->
        broadcast(:shapes, %{owner_id: id}, :bare, at_runtime(%{}))
      end
    end

    test "the from variants validate as well", %{owner_id: id} do
      assert_raise PayloadError, fn ->
        broadcast_from!(self(), :shapes, %{owner_id: id}, :bare, at_runtime(%{}))
      end

      assert_raise PayloadError, fn ->
        broadcast_from(self(), :shapes, %{owner_id: id}, :bare, at_runtime(%{}))
      end
    end
  end

  describe "compile-time checks on a literal payload" do
    defp source(payload) do
      """
      defmodule #{unique_module("VPTest.Payload")} do
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
        def go(id), do: broadcast!(:shapes, %{owner_id: id}, :bare, #{payload})
      end
      """
    end

    test "a valid literal payload compiles" do
      assert is_atom(compile!(source(~s|%{id: "x"}|)))
    end

    test "a missing required key fails the compile, listing the declared fields" do
      error = compile_error(source("%{}"))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "missing required: [:id]"
      assert message =~ "field :id, :string"
    end

    test "an unexpected key fails the compile" do
      error = compile_error(source(~s|%{id: "x", nope: 1}|))

      assert %CompileError{} = error
      assert Exception.message(error) =~ "unexpected: [:nope]"
    end

    test "a literal value of the wrong type fails the compile" do
      error = compile_error(source("%{id: 42}"))

      assert %CompileError{} = error
      assert Exception.message(error) =~ ":id is declared as :string, got: 42"
    end

    test "a literal nil for a required field fails the compile" do
      error = compile_error(source("%{id: nil}"))

      assert %CompileError{} = error
      assert Exception.message(error) =~ ":id is declared as :string, got: nil"
    end

    test "a non-literal value is left to the runtime check" do
      assert is_atom(compile!(source("%{id: to_string(id)}")))
    end

    test "a map-update expression is not mistaken for a literal map" do
      # `%{base | id: "x"}` is a single {:|, _, _} tuple, not key/value pairs. Reading it
      # as a literal map would report a bogus unexpected key :| on correct code.
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.MapUpdate")} do
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

                 def go(id, base) do
                   broadcast!(:shapes, %{owner_id: id}, :bare, %{base | id: "x"})
                 end
               end
               """)
             )
    end

    test "a map-update params expression is not mistaken for a literal map either" do
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.MapUpdateParams")} do
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

                 def go(base) do
                   broadcast!(:shapes, %{base | owner_id: "1"}, :bare, %{id: "x"})
                 end
               end
               """)
             )
    end
  end

  describe "field type declarations" do
    test "an unknown type is a compile error listing the valid ones" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.BadType")} do
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

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
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

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
                 use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

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
