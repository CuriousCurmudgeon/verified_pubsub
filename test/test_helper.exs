# One Phoenix.PubSub for the whole suite, since a named process cannot be started
# per-file by concurrent async tests. Tests that subscribe must therefore use a unique
# param value (see `VerifiedPubsub.CompileHelper.unique_account_id/0`) so subscriptions
# never collide across files.
{:ok, _} =
  Supervisor.start_link([{Phoenix.PubSub, name: VerifiedPubsub.TestPubSub}],
    strategy: :one_for_one
  )

ExUnit.start()
