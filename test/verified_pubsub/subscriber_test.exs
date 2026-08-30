defmodule VerifiedPubSub.SubscriberTest do
  use ExUnit.Case, async: true

  use VerifiedPubSub, registry: VerifiedPubSub.TestRegistries.Basic

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message

  defmodule Worker do
    use GenServer

    use VerifiedPubSub.Subscriber,
      registry: VerifiedPubSub.TestRegistries.Basic

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      :ok = subscribe(:campaigns, %{account_id: opts[:account_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    handle_message :campaigns, :created, payload, state do
      send(state.owner, {:created, payload})
      {:noreply, state}
    end

    handle_message :campaigns,
                   :updated,
                   %Message{params: %{account_id: account_id}, payload: payload},
                   state do
      send(state.owner, {:updated, account_id, payload})
      {:noreply, state}
    end

    ignore_message :campaigns, :deleted
  end

  defmodule WorkerWithOwnHandleInfo do
    use GenServer

    use VerifiedPubSub.Subscriber,
      registry: VerifiedPubSub.TestRegistries.Basic

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      :ok = subscribe(:campaigns, %{account_id: opts[:account_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    @impl true
    def handle_info(:tick, state) do
      send(state.owner, :ticked)
      {:noreply, state}
    end

    handle_message :campaigns, :created, payload, state do
      send(state.owner, {:created, payload})
      {:noreply, state}
    end

    ignore_message :campaigns, :updated
    ignore_message :campaigns, :deleted
  end

  defmodule WorkerWithCatchAll do
    use GenServer

    use VerifiedPubSub.Subscriber,
      registry: VerifiedPubSub.TestRegistries.Basic

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      :ok = subscribe(:campaigns, %{account_id: opts[:account_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    handle_message :campaigns, :created, payload, state do
      send(state.owner, {:created, payload})
      {:noreply, state}
    end

    ignore_message :campaigns, :updated
    ignore_message :campaigns, :deleted

    @impl true
    def handle_info(other, state) do
      send(state.owner, {:catchall, other})
      {:noreply, state}
    end
  end

  setup do
    id = unique_account_id()
    pid = start_supervised!({Worker, owner: self(), account_id: id})
    %{account_id: id, worker: pid}
  end

  test "the payload form receives the payload", %{account_id: id} do
    broadcast!(:campaigns, :created, %{account_id: id}, %{id: "c1"})

    assert_receive {:created, %{id: "c1"}}
  end

  test "the Message form can match on topic params", %{account_id: id} do
    broadcast!(:campaigns, :updated, %{account_id: id}, %{id: "c2"})

    assert_receive {:updated, ^id, %{id: "c2"}}
  end

  test "an ignored event is received without crashing", %{account_id: id, worker: pid} do
    broadcast!(:campaigns, :deleted, %{account_id: id}, %{id: "c3"})

    refute_receive {:created, _}, 50
    refute_receive {:updated, _, _}, 50
    assert Process.alive?(pid)
  end

  test "exactly one handle_info clause is generated for verified messages" do
    assert function_exported?(Worker, :handle_info, 2)
  end

  @tag :capture_log
  test "an unrelated message raises unless the module handles it", %{worker: pid} do
    # Defining any handle_info/2 discards the default that `use GenServer` installs,
    # and Elixir 1.20 made super/2 for GenServer callbacks a hard error, so that
    # default cannot be preserved. This matches any GenServer with a custom
    # handle_info/2. Subscribers receiving other messages add a catch-all.
    ref = Process.monitor(pid)
    send(pid, :something_unrelated)

    # The exit reason is the raw Erlang form, not a %FunctionClauseError{} struct.
    assert_receive {:DOWN, ^ref, :process, ^pid, {:function_clause, _stacktrace}}
  end

  test "a module-defined catch-all handles unrelated messages without shadowing" do
    id = unique_account_id()
    pid = start_supervised!({WorkerWithCatchAll, owner: self(), account_id: id})

    send(pid, :something_unrelated)
    assert_receive {:catchall, :something_unrelated}
    assert Process.alive?(pid)

    # The generated clause is emitted at the `use` site, so it is matched before the
    # user's catch-all and verified messages still dispatch correctly.
    broadcast!(:campaigns, :created, %{account_id: id}, %{id: "c7"})
    assert_receive {:created, %{id: "c7"}}
  end

  test "coexists with a user-defined handle_info clause" do
    id = unique_account_id()
    pid = start_supervised!({WorkerWithOwnHandleInfo, owner: self(), account_id: id})

    send(pid, :tick)
    assert_receive :ticked

    broadcast!(:campaigns, :created, %{account_id: id}, %{id: "c9"})
    assert_receive {:created, %{id: "c9"}}

    assert Process.alive?(pid)
  end
end
