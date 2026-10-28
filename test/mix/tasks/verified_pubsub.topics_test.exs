defmodule Mix.Tasks.VerifiedPubsub.TopicsTest do
  # Not async, because the Mix shell is global.
  use ExUnit.Case, async: false

  alias Mix.Tasks.VerifiedPubsub.Topics

  @manifest ["--manifest", "VerifiedPubSub.UsageFixtures.Topics"]

  setup do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)
  end

  describe "mix verified_pubsub.topics" do
    test "prints each call site of each topic by default" do
      output = run(@manifest)

      assert output =~ "VerifiedPubSub.UsageFixtures.Topics\n\n:kennel  kennels:<kennel_id>\n"
      assert output =~ "\n:walks  walks\n\nSubscribes: none\n"

      assert output =~
               ~r/broadcast\s+test\/support\/usage_fixtures.ex:\d+\s+VerifiedPubSub.UsageFixtures.Kennel/
    end

    test "prints the summary table with --summary" do
      output = run(["--summary" | @manifest])

      assert output =~ "VerifiedPubSub.UsageFixtures.Topics\n"
      assert output =~ ~r/^:kennel\s+kennels:<kennel_id>\s+2\s+1\s+1\s+1$/m
      assert output =~ ~r/^:walks\s+walks\s+1\s+1\s+0\s+0$/m
    end

    test "prints each call site of a topic" do
      output = run(["kennel" | @manifest])

      assert output =~ "VerifiedPubSub.UsageFixtures.Topics\n\n:kennel  kennels:<kennel_id>\n"

      assert output =~
               ~r/subscribe\s+test\/support\/usage_fixtures.ex:\d+\s+VerifiedPubSub.UsageFixtures.Board/

      assert output =~
               ~r/broadcast\s+test\/support\/usage_fixtures.ex:\d+\s+VerifiedPubSub.UsageFixtures.Kennel/

      assert output =~
               ~r/handle_message\s+test\/support\/usage_fixtures.ex:\d+\s+VerifiedPubSub.UsageFixtures.Board/

      assert output =~ ":checked_out  0 broadcasts, 1 ignore  (no broadcast)"
    end

    test "prints the row of one topic with --summary" do
      output = run(["kennel", "--summary" | @manifest])

      assert output =~ ~r/^:kennel\s+kennels:<kennel_id>\s+2\s+1\s+1\s+1$/m
      refute output =~ ":walks"
    end

    test "prints the counts of one event in the row of its topic with --summary" do
      output = run(["kennel", "checked_out", "--summary" | @manifest])

      assert output =~ ~r/^:kennel\s+kennels:<kennel_id>\s+1\s+0\s+0\s+1$/m
    end

    test "prints one event" do
      output = run(["kennel", "checked_in" | @manifest])

      assert output =~ ":checked_in  1 broadcast, 1 handler"
      assert output =~ "Subscribes (1)"
      refute output =~ ":checked_out"
    end

    @tag :json
    test "prints JSON with --format json" do
      output = run(["kennel", "--format", "json" | @manifest])

      assert %{"manifests" => [%{"topics" => [%{"name" => "kennel", "events" => events}]}]} =
               JSON.decode!(output)

      assert Enum.map(events, & &1["name"]) == ["checked_in", "checked_out"]
    end

    test "keeps the output of the compile out of --format json" do
      Topics.quietly("json", fn -> Mix.shell().info("Compiling 1 file (.ex)") end)
      refute_received {:mix_shell, :info, _}

      Topics.quietly("text", fn -> Mix.shell().info("Compiling 1 file (.ex)") end)
      assert_received {:mix_shell, :info, ["Compiling 1 file (.ex)"]}
    end

    test "raises for an unknown topic" do
      assert_raise Mix.Error, ~r/unknown topic :petz/, fn -> run(["petz" | @manifest]) end
    end

    test "raises for an unknown format" do
      assert_raise Mix.Error, ~r/unknown format "xml".*text.*json/s, fn ->
        run(["--format", "xml" | @manifest])
      end
    end

    test "raises for more than two arguments" do
      assert_raise Mix.Error, ~r/mix verified_pubsub.topics \[TOPIC \[EVENT\]\]/, fn ->
        run(["kennel", "checked_in", "extra" | @manifest])
      end
    end
  end

  defp run(args) do
    Topics.run(args)
    assert_received {:mix_shell, :info, [output]}
    output
  end
end
