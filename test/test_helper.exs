# The whole suite uses one Phoenix.PubSub, because async tests run at the same time and
# cannot each start a named process for their file. So a test that subscribes uses a
# unique param value (see `VerifiedPubSub.CompileHelper.unique_owner_id/0`), and the
# subscriptions of different files never collide.
{:ok, _} =
  Supervisor.start_link([{Phoenix.PubSub, name: VerifiedPubSub.TestPubSub}],
    strategy: :one_for_one
  )

ExUnit.start()
