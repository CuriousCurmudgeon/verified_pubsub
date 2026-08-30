# The Local adapter's default registry is started once for the whole suite, because a
# named process cannot be started per-file by concurrent async tests. Tests that
# subscribe must therefore use a unique param value (see `unique_account_id/0` in
# VerifiedPubsub.CompileHelper) so subscriptions never collide across files.
{:ok, _} = Supervisor.start_link([VerifiedPubsub.Adapter.Local], strategy: :one_for_one)

ExUnit.start()
