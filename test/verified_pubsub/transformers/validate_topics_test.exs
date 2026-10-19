defmodule VerifiedPubSub.Transformers.ValidateTopicsTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  defp manifest_source(body) do
    """
    defmodule #{unique_module("VPTest.Validate")} do
      use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub
      #{body}
    end
    """
  end

  describe "topic" do
    test "a valid manifest compiles" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :pets, ["owners", :owner_id, "pets"] do
                   message :adopted do
                     field :id, :string
                   end
                 end
                 """)
               )
             )
    end

    test "a duplicate topic name is a compile error that names the topic and the first declaration" do
      error =
        dsl_error(
          manifest_source("""
          topic :pets, ["a"] do
            message :adopted do
            end
          end

          topic :pets, ["b"] do
            message :adopted do
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "duplicate topic"
      assert Exception.message(error) =~ "pets"
      assert Exception.message(error) =~ "The first is declared at nofile:3."
      assert :erl_anno.line(error.location) == 8
    end

    test "malformed segments are a compile error that says to give each part its own list element" do
      error =
        dsl_error(
          manifest_source("""
          topic :pets, ["owners:pets"] do
            message :adopted do
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "its own list element"
    end

    test "a segment that is neither a string nor an atom is a compile error" do
      error =
        dsl_error(
          manifest_source("""
          topic :pets, ["owners", 1] do
            message :adopted do
            end
          end
          """)
        )

      assert error, "expected a non-string, non-atom segment to fail compilation"
    end

    test "a topic with no events is a compile error" do
      error =
        dsl_error(
          manifest_source("""
          topic :pets, ["a"] do
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "declares no events"
    end

    test "a param that fills the whole topic is fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :t, [:id] do
                   message :e do
                     field :id, :string
                   end
                 end
                 """)
               )
             )
    end
  end

  describe "message" do
    test "a duplicate event name within a topic is a compile error that names the event" do
      error =
        dsl_error(
          manifest_source("""
          topic :pets, ["a"] do
            message :adopted do
            end

            message :adopted do
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "duplicate event"
      assert Exception.message(error) =~ "adopted"
    end

    test "the same event name on two different topics compiles" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :pets, ["a"] do
                   message :adopted do
                   end
                 end

                 topic :treats, ["b"] do
                   message :adopted do
                   end
                 end
                 """)
               )
             )
    end
  end

  describe "use VerifiedPubSub.Manifest with topics of a similar shape" do
    test "two topics that can match the same wire topic are a compile error that names both topics and their declarations" do
      error =
        dsl_error(
          manifest_source("""
          topic :pets, ["owners", :owner_id, "pets"] do
            message :adopted do
              field :id, :string
            end
          end

          topic :scoped, ["owners", :owner_id, :thing] do
            message :touched do
              field :id, :string
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      message = Exception.message(error)
      assert message =~ "can match the same topic"
      assert message =~ ~r/:pets — .*, at nofile:3$/m
      assert message =~ ~r/:scoped — .*, at nofile:9$/m
      assert :erl_anno.line(error.location) == 3
    end

    test "the error is also a diagnostic at the first topic" do
      {_error, diagnostics} =
        Code.with_diagnostics(fn ->
          compile_error(
            manifest_source("""
            topic :pets, ["owners", :owner_id, "pets"] do
              message :adopted do
              end
            end

            topic :scoped, ["owners", :owner_id, :thing] do
              message :touched do
              end
            end
            """)
          )
        end)

      assert [%{severity: :warning, position: 3, message: message}] = diagnostics
      assert message =~ "can match the same topic"
    end

    test "topics that differ by a literal segment are fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :pets, ["owners", :owner_id, "pets"] do
                   message :adopted do
                     field :id, :string
                   end
                 end

                 topic :treats, ["owners", :owner_id, "treats"] do
                   message :adopted do
                     field :id, :string
                   end
                 end
                 """)
               )
             )
    end

    test "topics of different segment counts are fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :short, ["a", :x] do
                   message :e do
                     field :id, :string
                   end
                 end

                 topic :long, ["a", :x, "b"] do
                   message :e do
                     field :id, :string
                   end
                 end
                 """)
               )
             )
    end
  end
end
