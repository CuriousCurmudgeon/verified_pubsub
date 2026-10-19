defmodule VerifiedPubSub.SubscriberVerifyTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  defp subscriber_source(body) do
    """
    defmodule #{unique_module("VPTest.Sub")} do
      use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

      #{body}
    end
    """
  end

  defp all_handled do
    """
    handle_message :pets, :adopted, p, s do
      {:noreply, {p, s}}
    end

    handle_message :pets, :renamed, p, s do
      {:noreply, {p, s}}
    end

    handle_message :pets, :rehomed, p, s do
      {:noreply, {p, s}}
    end
    """
  end

  describe "use VerifiedPubSub.Subscriber" do
    test "a subscriber that covers every event compiles" do
      assert is_atom(compile!(subscriber_source(all_handled())))
    end

    test "a missing event is a compile error that names the event and ignore_message/2" do
      error =
        compile_error(
          subscriber_source("""
          handle_message :pets, :adopted, p, s do
            {:noreply, {p, s}}
          end
          """)
        )

      assert error, "expected a missing event to fail compilation"
      message = Exception.message(error)
      assert message =~ ":renamed"
      assert message =~ ":rehomed"
      assert message =~ "ignore_message"
    end

    test "ignore_message/2 covers an event" do
      assert is_atom(
               compile!(
                 subscriber_source("""
                 handle_message :pets, :adopted, p, s do
                   {:noreply, {p, s}}
                 end

                 ignore_message :pets, :renamed
                 ignore_message :pets, :rehomed
                 """)
               )
             )
    end

    test "a handler for an event that the topic does not declare is a compile error" do
      error =
        compile_error(
          subscriber_source("""
          #{all_handled()}

          handle_message :pets, :exploded, p, s do
            {:noreply, {p, s}}
          end
          """)
        )

      assert error, "expected an undeclared event to fail compilation"
      assert Exception.message(error) =~ ":exploded"
    end

    test "a handler for a second topic subscribes to it, and the module must cover it too" do
      # :system declares exactly one event, so a handler for it gives full coverage.
      assert is_atom(
               compile!(
                 subscriber_source("""
                 #{all_handled()}

                 handle_message :system, :alert, p, s do
                   {:noreply, {p, s}}
                 end
                 """)
               )
             )
    end

    test "an inferred second topic with an uncovered event is a compile error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.TwoTopics")} do
          use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

          handle_message :system, :alert, p, s do
            {:noreply, {p, s}}
          end

          handle_message :pets, :adopted, p, s do
            {:noreply, {p, s}}
          end
        end
        """)

      assert error, "expected the inferred :pets topic to require full coverage"
      message = Exception.message(error)
      assert message =~ ":renamed"
      assert message =~ ":rehomed"
    end

    test "the removed :topics option gives a migration error" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.OldTopics")} do
          use VerifiedPubSub.Subscriber,
            manifest: VerifiedPubSub.TestManifests.Basic,
            topics: [:pets]
        end
        """)

      assert error, "expected :topics to be rejected"
      message = Exception.message(error)
      assert message =~ ":topics option was removed"
      assert message =~ "inferred"
    end

    test "a handler and an ignore_message/2 for the same event are a compile error" do
      error =
        compile_error(
          subscriber_source("""
          handle_message :pets, :adopted, p, s do
            {:noreply, {p, s}}
          end

          ignore_message :pets, [:adopted, :renamed, :rehomed]
          """)
        )

      assert error, "expected a contradictory declaration to fail compilation"
      message = Exception.message(error)
      assert message =~ "both handles and ignores"
      assert message =~ ":adopted"
    end

    test "ignore_message/2 with a list reports the events that the list leaves out" do
      error =
        compile_error(
          subscriber_source("""
          handle_message :pets, :adopted, p, s do
            {:noreply, {p, s}}
          end

          ignore_message :pets, [:renamed]
          """)
        )

      assert error, "expected the uncovered :rehomed event to fail compilation"
      assert Exception.message(error) =~ ":rehomed"
    end

    test "a module can have several clauses for one event" do
      # This test keeps a past bug from coming back. With list subtraction, the duplicate
      # {:pets, :adopted} leaves a residue, and the check reports it as undeclared.
      assert is_atom(
               compile!(
                 subscriber_source("""
                 handle_message :pets, :adopted, %{id: "special"}, s do
                   {:noreply, s}
                 end

                 handle_message :pets, :adopted, p, s do
                   {:noreply, {p, s}}
                 end

                 ignore_message :pets, :renamed
                 ignore_message :pets, :rehomed
                 """)
               )
             )
    end

    test "an unknown option is rejected" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.BadOpts")} do
          use VerifiedPubSub.Subscriber,
            manifest: VerifiedPubSub.TestManifests.Basic,
            bogus: true
        end
        """)

      assert error, "expected an unknown option to fail compilation"
      assert Exception.message(error) =~ "bogus"
    end
  end

  describe "handle_message/5" do
    test "a misspelled topic is a compile error that names the declared topics" do
      error =
        compile_error("""
        defmodule #{unique_module("VPTest.TypoTopic")} do
          use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic

          handle_message :pet, :adopted, p, s do
            {:noreply, {p, s}}
          end
        end
        """)

      assert error, "expected a typo'd topic to fail compilation"
      message = Exception.message(error)
      assert message =~ "unknown topic :pet"

      assert message =~
               ~r{:pets\s+owners:<owner_id>:pets\s+test/support/manifests\.ex:\d+}
    end

    test "a topic that is not a literal is a compile error" do
      error =
        compile_error(
          subscriber_source("""
          @t :pets
          handle_message @t, :adopted, p, s do
            {:noreply, {p, s}}
          end
          """)
        )

      assert error, "expected a non-literal topic to fail compilation"
      assert Exception.message(error) =~ "literal"
    end
  end

  describe "ignore_message/2" do
    test "accepts a list of events" do
      assert is_atom(
               compile!(
                 subscriber_source("""
                 handle_message :pets, :adopted, p, s do
                   {:noreply, {p, s}}
                 end

                 ignore_message :pets, [:renamed, :rehomed]
                 """)
               )
             )
    end

    test "an empty list is rejected" do
      error =
        compile_error(
          subscriber_source("""
          #{all_handled()}

          ignore_message :pets, []
          """)
        )

      assert error, "expected an empty ignore list to be rejected"
      assert Exception.message(error) =~ "ignores nothing"
    end
  end
end
