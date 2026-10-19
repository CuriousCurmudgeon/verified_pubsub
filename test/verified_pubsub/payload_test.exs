defmodule VerifiedPubSub.PayloadTest do
  use ExUnit.Case, async: true
  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.Payload
  alias VerifiedPubSub.PayloadError
  alias VerifiedPubSub.TestManifests.Basic
  alias VerifiedPubSub.TestStructs.Point

  import VerifiedPubSub.CompileHelper

  setup do
    %{x: unique_owner_id()}
  end

  # A function call hides a payload from the literal check at compile time, so the
  # runtime check runs. Separate tests below cover literal payloads. This function returns
  # its argument on purpose: it exists only to hide the payload from AST inspection.
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

  describe "Payload.validate!/4" do
    # `validate!/4` returns the payload that it gets. So an assertion on the return value
    # states both parts of the claim: the payload is accepted, and it does not change.
    defp accept!(event, payload) do
      assert payload == Payload.validate!(Basic, :typed_payloads, event, payload)
    end

    test "every declared type accepts a matching value, with the optional field omitted" do
      accept!(:typed, valid_typed())
    end

    test "an optional field can be present" do
      accept!(:typed, Map.put(valid_typed(), :note, "hi"))
    end

    test "an optional field can be nil" do
      accept!(:typed, Map.put(valid_typed(), :note, nil))
    end

    test "a struct field accepts the declared struct" do
      accept!(:structured, %{point: %Point{x: 1, y: 2}})
    end

    test "an :any field accepts any value" do
      accept!(:typed, Map.put(valid_typed(), :anything, self()))
      accept!(:typed, Map.put(valid_typed(), :anything, nil))
    end
  end

  describe "broadcast!/4 with a payload built at runtime" do
    # One test goes through the macro, and covers the whole path: the check accepts the
    # payload, `Payload.validate!/4` returns it, and the macro puts that return value on
    # the message. The tests of accepted values above call the validator directly. The
    # delivery has no part in what they claim, and an assertion on it there hides the
    # claim.
    test "a valid payload is delivered unchanged", %{x: id} do
      assert :ok = subscribe(:typed_payloads, %{x: id})
      payload = valid_typed()
      assert :ok = broadcast!(:typed_payloads, %{x: id}, :typed, payload)

      assert_receive %Message{event: :typed, payload: ^payload}
    end

    test "a missing required key raises and names it", %{x: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :bare, at_runtime(%{}))
        end

      message = Exception.message(error)
      assert message =~ "missing required key: :id"
      assert message =~ ":typed_payloads"
      assert message =~ ":bare"
    end

    test "an unexpected key raises and names it", %{x: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :bare, at_runtime(%{id: "x", nope: 1}))
        end

      assert Exception.message(error) =~ "unexpected key: :nope"
    end

    test "a wrong type raises an error that names the field, the declared type, and the value", %{
      x: id
    } do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :bare, at_runtime(%{id: 42}))
        end

      message = Exception.message(error)
      assert message =~ ":id is declared as :string"
      assert message =~ "got: 42"
    end

    test "the error reports every problem at once, not only the first", %{x: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(
            :typed_payloads,
            %{x: id},
            :typed,
            at_runtime(%{count: "not an int", nope: 1})
          )
        end

      message = Exception.message(error)
      assert message =~ "missing required keys:"
      assert message =~ "unexpected key: :nope"
      assert message =~ ":count is declared as :integer"
    end

    test "a {:list, :string} rejects a list with a bad element", %{x: id} do
      payload = Map.put(valid_typed(), :tags, ["ok", 1])

      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :typed, at_runtime(payload))
        end

      assert Exception.message(error) =~ "{:list, :string}"
    end

    test "a struct field rejects a different struct", %{x: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :structured, %{
            point: %Message{
              manifest: Basic,
              topic: :a,
              event: :b
            }
          })
        end

      assert Exception.message(error) =~ "VerifiedPubSub.TestStructs.Point"
    end

    test "a non-map payload raises", %{x: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :bare, at_runtime("not a map"))
        end

      assert Exception.message(error) =~ "must be a map of the declared fields"
    end

    test "the error for a struct payload explains how to carry the struct in a field", %{x: id} do
      error =
        assert_raise PayloadError, fn ->
          broadcast!(:typed_payloads, %{x: id}, :bare, at_runtime(%Point{x: 1, y: 2}))
        end

      message = Exception.message(error)
      assert message =~ "is a VerifiedPubSub.TestStructs.Point struct"
      assert message =~ "Declare a field to carry it"
    end

    test "broadcasts nothing when the payload is not valid", %{x: id} do
      assert :ok = subscribe(:typed_payloads, %{x: id})

      assert_raise PayloadError, fn ->
        broadcast!(:typed_payloads, %{x: id}, :bare, at_runtime(%{}))
      end

      refute_receive %Message{topic: :typed_payloads}, 50
    end
  end

  describe "broadcast/4" do
    test "raises on a bad payload, as broadcast!/4 does", %{x: id} do
      # A wrong shape is a bug, not a transport failure. So the macro does not return it
      # as an {:error, _} tuple, which a caller can ignore.
      assert_raise PayloadError, fn ->
        broadcast(:typed_payloads, %{x: id}, :bare, at_runtime(%{}))
      end
    end
  end

  describe "broadcast_from/5, broadcast_from!/5" do
    test "validate the payload", %{x: id} do
      assert_raise PayloadError, fn ->
        broadcast_from!(self(), :typed_payloads, %{x: id}, :bare, at_runtime(%{}))
      end

      assert_raise PayloadError, fn ->
        broadcast_from(self(), :typed_payloads, %{x: id}, :bare, at_runtime(%{}))
      end
    end
  end

  describe "broadcast!/4 with a literal payload" do
    defp source(payload) do
      """
      defmodule #{unique_module("VPTest.Payload")} do
        use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic
        def go(id), do: broadcast!(:typed_payloads, %{x: id}, :bare, #{payload})
      end
      """
    end

    test "a valid literal payload compiles" do
      assert is_atom(compile!(source(~s|%{id: "x"}|)))
    end

    test "a missing required key is a compile error that lists the declared fields" do
      error = compile_error(source("%{}"))

      assert %CompileError{} = error
      message = Exception.message(error)
      assert message =~ "missing required: [:id]"
      assert message =~ "field :id, :string"
      assert message =~ ~r{Declared at test/support/manifests\.ex:\d+:}
    end

    test "an unexpected key is a compile error that names the key" do
      error = compile_error(source(~s|%{id: "x", nope: 1}|))

      assert %CompileError{} = error
      assert Exception.message(error) =~ "unexpected: [:nope]"
    end

    test "a literal value of the wrong type is a compile error that names the field, the declared type, and the value" do
      error = compile_error(source("%{id: 42}"))

      assert %CompileError{} = error
      assert Exception.message(error) =~ ":id is declared as :string, got: 42"
    end

    test "a literal nil for a required field is a compile error that names the field and its declared type" do
      error = compile_error(source("%{id: nil}"))

      assert %CompileError{} = error
      assert Exception.message(error) =~ ":id is declared as :string, got: nil"
    end

    test "the runtime check handles a value that is not a literal" do
      assert is_atom(compile!(source("%{id: to_string(id)}")))
    end

    test "a map update expression does not count as a literal map" do
      # `%{base | id: "x"}` is one {:|, _, _} tuple, not key/value pairs. A check that
      # reads it as a literal map reports a false unexpected key :| on correct code.
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.MapUpdate")} do
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

                 def go(id, base) do
                   broadcast!(:typed_payloads, %{x: id}, :bare, %{base | id: "x"})
                 end
               end
               """)
             )
    end

    test "a map update expression for the params does not count as a literal map" do
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.MapUpdateParams")} do
                 use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

                 def go(base) do
                   broadcast!(:typed_payloads, %{base | x: "1"}, :bare, %{id: "x"})
                 end
               end
               """)
             )
    end
  end

  describe "field :type" do
    test "an unknown type is a compile error that lists the valid types" do
      error =
        dsl_error("""
        defmodule #{unique_module("VPTest.BadType")} do
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

          topic :t, ["t"] do
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
        dsl_error("""
        defmodule #{unique_module("VPTest.BadInner")} do
          use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

          topic :t, ["t"] do
            message :e do
              field :tags, {:list, :strng}
            end
          end
        end
        """)

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "invalid type"
    end

    test "a struct module does not need to be loaded" do
      assert is_atom(
               compile!("""
               defmodule #{unique_module("VPTest.StructType")} do
                 use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

                 topic :t, ["t"] do
                   message :e do
                     field :thing, SomeApp.NotCompiledYet
                   end
                 end
               end
               """)
             )
    end
  end

  describe "Info.fields/3" do
    test "exposes the declared fields" do
      assert [%{name: :id, type: :string, required: true}] =
               VerifiedPubSub.Info.fields(Basic, :typed_payloads, :bare)
    end

    test "reports required: false" do
      note =
        Enum.find(VerifiedPubSub.Info.fields(Basic, :typed_payloads, :typed), &(&1.name == :note))

      assert note.required == false
    end

    test "raises for an unknown event" do
      assert_raise ArgumentError, ~r/unknown event :nope/, fn ->
        VerifiedPubSub.Info.fields(Basic, :typed_payloads, :nope)
      end
    end
  end
end
