defmodule VerifiedPubSub.SubscriberVerifyTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  defp subscriber_source(body, opts \\ "") do
    """
    defmodule #{unique_module("VPTest.Sub")} do
      use VerifiedPubSub.Subscriber,
        registry: VerifiedPubSub.TestRegistries.Basic,
        topics: [:campaigns]#{opts}

      #{body}
    end
    """
  end

  defp all_handled do
    """
    handle_message :campaigns, :created, p, s do
      {:noreply, {p, s}}
    end

    handle_message :campaigns, :updated, p, s do
      {:noreply, {p, s}}
    end

    handle_message :campaigns, :deleted, p, s do
      {:noreply, {p, s}}
    end
    """
  end

  test "a fully-covered subscriber compiles" do
    assert is_atom(compile!(subscriber_source(all_handled())))
  end

  test "a missing event is a compile error naming the event and the escape hatch" do
    error =
      compile_error(
        subscriber_source("""
        handle_message :campaigns, :created, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected a missing event to fail compilation"
    message = Exception.message(error)
    assert message =~ ":updated"
    assert message =~ ":deleted"
    assert message =~ "ignore_message"
  end

  test "ignore_message satisfies coverage" do
    assert is_atom(
             compile!(
               subscriber_source("""
               handle_message :campaigns, :created, p, s do
                 {:noreply, {p, s}}
               end

               ignore_message :campaigns, :updated
               ignore_message :campaigns, :deleted
               """)
             )
           )
  end

  test "handling an event not declared on the topic is a compile error" do
    error =
      compile_error(
        subscriber_source("""
        #{all_handled()}

        handle_message :campaigns, :exploded, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected an undeclared event to fail compilation"
    assert Exception.message(error) =~ ":exploded"
  end

  test "handling a topic that was not subscribed to is a compile error" do
    error =
      compile_error(
        subscriber_source("""
        #{all_handled()}

        handle_message :system, :alert, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected an unsubscribed topic to fail compilation"
    assert Exception.message(error) =~ ":system"
  end

  test "several clauses for one event are allowed" do
    # Regression guard for set semantics: with list subtraction the duplicate
    # {:campaigns, :created} would leave a residue and be misreported as undeclared.
    assert is_atom(
             compile!(
               subscriber_source("""
               handle_message :campaigns, :created, %{id: "special"}, s do
                 {:noreply, s}
               end

               handle_message :campaigns, :created, p, s do
                 {:noreply, {p, s}}
               end

               ignore_message :campaigns, :updated
               ignore_message :campaigns, :deleted
               """)
             )
           )
  end

  test "on_missing: :ignore skips the check" do
    assert is_atom(
             compile!(
               subscriber_source(
                 """
                 handle_message :campaigns, :created, p, s do
                   {:noreply, {p, s}}
                 end
                 """,
                 ",\n    on_missing: :ignore"
               )
             )
           )
  end

  test "on_missing: :warn warns instead of raising" do
    {result, diagnostics} =
      Code.with_diagnostics(fn ->
        compile_error(
          subscriber_source(
            """
            handle_message :campaigns, :created, p, s do
              {:noreply, {p, s}}
            end
            """,
            ",\n    on_missing: :warn"
          )
        )
      end)

    refute result, "expected on_missing: :warn not to raise"

    assert Enum.any?(diagnostics, &(&1.severity == :warning and &1.message =~ ":updated")),
           "expected a warning naming the missing event, got: #{inspect(diagnostics)}"
  end

  test "a non-literal topic is rejected" do
    error =
      compile_error(
        subscriber_source("""
        @t :campaigns
        handle_message @t, :created, p, s do
          {:noreply, {p, s}}
        end
        """)
      )

    assert error, "expected a non-literal topic to fail compilation"
    assert Exception.message(error) =~ "literal"
  end

  test "an unknown option to use is rejected" do
    error =
      compile_error("""
      defmodule #{unique_module("VPTest.BadOpts")} do
        use VerifiedPubSub.Subscriber,
          registry: VerifiedPubSub.TestRegistries.Basic,
          topics: [:campaigns],
          bogus: true
      end
      """)

    assert error, "expected an unknown option to fail compilation"
    assert Exception.message(error) =~ "bogus"
  end
end
