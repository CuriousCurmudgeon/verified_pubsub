defmodule VerifiedPubSub.Transformers.ValidateTopicsTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  defp registry_source(body) do
    """
    defmodule #{unique_module("VPTest.Validate")} do
      use VerifiedPubSub.Registry, pubsub: VerifiedPubSub.TestPubSub
      #{body}
    end
    """
  end

  test "a valid registry compiles" do
    assert is_atom(
             compile!(
               registry_source("""
               topic :campaigns, "accounts:%{account_id}:campaigns" do
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
        registry_source("""
        topic :campaigns, "a" do
          message :created do
          end
        end

        topic :campaigns, "b" do
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
        registry_source("""
        topic :campaigns, "a" do
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
               registry_source("""
               topic :campaigns, "a" do
                 message :created do
                 end
               end

               topic :contacts, "b" do
                 message :created do
                 end
               end
               """)
             )
           )
  end

  test "a malformed pattern is a compile error" do
    error =
      compile_error(
        registry_source("""
        topic :campaigns, "a:%{oops" do
          message :created do
          end
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "unterminated"
  end

  test "a topic with no events is a compile error" do
    error =
      compile_error(
        registry_source("""
        topic :campaigns, "a" do
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "declares no events"
  end
end
