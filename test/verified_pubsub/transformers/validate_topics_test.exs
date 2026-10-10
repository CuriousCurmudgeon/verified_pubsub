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

  test "a valid manifest compiles" do
    assert is_atom(
             compile!(
               manifest_source("""
               topic :campaigns, ["accounts", :account_id, "campaigns"] do
                 message :created do
                   field :id, :string
                 end
               end
               """)
             )
           )
  end

  test "duplicate topic names are a compile error" do
    error =
      compile_error(
        manifest_source("""
        topic :campaigns, ["a"] do
          message :created do
          end
        end

        topic :campaigns, ["b"] do
          message :created do
          end
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "duplicate topic"
    assert Exception.message(error) =~ "campaigns"
  end

  test "duplicate event names within a topic are a compile error" do
    error =
      compile_error(
        manifest_source("""
        topic :campaigns, ["a"] do
          message :created do
          end

          message :created do
          end
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "duplicate event"
    assert Exception.message(error) =~ "created"
  end

  test "the same event name on two different topics is allowed" do
    assert is_atom(
             compile!(
               manifest_source("""
               topic :campaigns, ["a"] do
                 message :created do
                 end
               end

               topic :contacts, ["b"] do
                 message :created do
                 end
               end
               """)
             )
           )
  end

  test "malformed segments are a compile error" do
    error =
      compile_error(
        manifest_source("""
        topic :campaigns, ["accounts:campaigns"] do
          message :created do
          end
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "its own list element"
  end

  test "a segment that is neither a string nor an atom is a compile error" do
    error =
      compile_error(
        manifest_source("""
        topic :campaigns, ["accounts", 1] do
          message :created do
          end
        end
        """)
      )

    assert error, "expected a non-string, non-atom segment to fail compilation"
  end

  test "a topic with no events is a compile error" do
    error =
      compile_error(
        manifest_source("""
        topic :campaigns, ["a"] do
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "declares no events"
  end

  test "a param filling the entire topic is fine" do
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

  describe "topic disjointness" do
    test "two topics that can match the same topic string are a compile error" do
      error =
        compile_error(
          manifest_source("""
          topic :campaigns, ["accounts", :account_id, "campaigns"] do
            message :created do
              field :id, :string
            end
          end

          topic :scoped, ["accounts", :account_id, :thing] do
            message :touched do
              field :id, :string
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      message = Exception.message(error)
      assert message =~ "can match the same topic"
      assert message =~ ":campaigns"
      assert message =~ ":scoped"
    end

    test "topics distinguished by a literal segment are fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :campaigns, ["accounts", :account_id, "campaigns"] do
                   message :created do
                     field :id, :string
                   end
                 end

                 topic :contacts, ["accounts", :account_id, "contacts"] do
                   message :created do
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
