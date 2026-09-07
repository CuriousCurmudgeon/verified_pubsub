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
        manifest_source("""
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
        manifest_source("""
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
               manifest_source("""
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
        manifest_source("""
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
        manifest_source("""
        topic :campaigns, "a" do
        end
        """)
      )

    assert %Spark.Error.DslError{} = error
    assert Exception.message(error) =~ "declares no events"
  end

  describe "pattern shape" do
    # A param must be a whole colon-delimited segment. Without this, a value can complete
    # a literal and forge a different topic: "a:x%{p}" with p = "y" builds "a:xy", which
    # is what the declared pattern "a:xy" builds. `~p` restricts path interpolation the
    # same way, for the same reason.
    test "a param that does not fill a whole segment is a compile error" do
      error =
        compile_error(
          manifest_source("""
          topic :campaigns, "accounts:acct%{account_id}:campaigns" do
            message :created do
              field :id, :string
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      message = Exception.message(error)
      assert message =~ "must be a whole"
      # The offending segment is quoted, which is what names the param.
      assert message =~ "acct%{account_id}"
    end

    test "two params in one segment are a compile error" do
      error =
        compile_error(
          manifest_source("""
          topic :campaigns, "accounts:%{org_id}%{account_id}" do
            message :created do
              field :id, :string
            end
          end
          """)
        )

      assert %Spark.Error.DslError{} = error
      assert Exception.message(error) =~ "must be a whole"
    end

    test "a param filling the entire pattern is fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :t, "%{id}" do
                   message :e do
                     field :id, :string
                   end
                 end
                 """)
               )
             )
    end
  end

  describe "pattern disjointness" do
    test "two patterns that can match the same topic string are a compile error" do
      error =
        compile_error(
          manifest_source("""
          topic :campaigns, "accounts:%{account_id}:campaigns" do
            message :created do
              field :id, :string
            end
          end

          topic :scoped, "accounts:%{account_id}:%{thing}" do
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

    test "patterns distinguished by a literal segment are fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :campaigns, "accounts:%{account_id}:campaigns" do
                   message :created do
                     field :id, :string
                   end
                 end

                 topic :contacts, "accounts:%{account_id}:contacts" do
                   message :created do
                     field :id, :string
                   end
                 end
                 """)
               )
             )
    end

    test "patterns of different segment counts are fine" do
      assert is_atom(
               compile!(
                 manifest_source("""
                 topic :short, "a:%{x}" do
                   message :e do
                     field :id, :string
                   end
                 end

                 topic :long, "a:%{x}:b" do
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
