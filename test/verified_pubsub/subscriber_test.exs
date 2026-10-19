defmodule VerifiedPubSub.SubscriberTest do
  use ExUnit.Case, async: true
  use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic

  alias VerifiedPubSub.Message

  import VerifiedPubSub.CompileHelper

  defmodule Worker do
    use GenServer

    use VerifiedPubSub.Subscriber,
      manifest: VerifiedPubSub.TestManifests.Basic

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      :ok = subscribe(:pets, %{owner_id: opts[:owner_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    handle_message :pets, :adopted, payload, state do
      send(state.owner, {:adopted, payload})
      {:noreply, state}
    end

    handle_message :pets,
                   :renamed,
                   %Message{params: %{owner_id: owner_id}, payload: payload},
                   state do
      send(state.owner, {:renamed, owner_id, payload})
      {:noreply, state}
    end

    ignore_message :pets, :rehomed
  end

  defmodule WorkerWithOwnHandleInfo do
    use GenServer

    use VerifiedPubSub.Subscriber,
      manifest: VerifiedPubSub.TestManifests.Basic

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      :ok = subscribe(:pets, %{owner_id: opts[:owner_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    @impl true
    def handle_info(:tick, state) do
      send(state.owner, :ticked)
      {:noreply, state}
    end

    handle_message :pets, :adopted, payload, state do
      send(state.owner, {:adopted, payload})
      {:noreply, state}
    end

    ignore_message :pets, :renamed
    ignore_message :pets, :rehomed
  end

  defmodule WorkerWithCatchAll do
    use GenServer

    use VerifiedPubSub.Subscriber,
      manifest: VerifiedPubSub.TestManifests.Basic

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      :ok = subscribe(:pets, %{owner_id: opts[:owner_id]})
      {:ok, %{owner: opts[:owner]}}
    end

    handle_message :pets, :adopted, payload, state do
      send(state.owner, {:adopted, payload})
      {:noreply, state}
    end

    ignore_message :pets, :renamed
    ignore_message :pets, :rehomed

    @impl true
    def handle_info(other, state) do
      send(state.owner, {:catchall, other})
      {:noreply, state}
    end
  end

  setup do
    id = unique_owner_id()
    pid = start_supervised!({Worker, owner: self(), owner_id: id})
    %{owner_id: id, worker: pid}
  end

  test "the payload form receives the payload", %{owner_id: id} do
    broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "c1"})

    assert_receive {:adopted, %{id: "c1"}}
  end

  test "the Message form can match on topic params", %{owner_id: id} do
    broadcast!(:pets, %{owner_id: id}, :renamed, %{id: "c2"})

    assert_receive {:renamed, ^id, %{id: "c2"}}
  end

  test "an ignored event does not crash the subscriber", %{owner_id: id, worker: pid} do
    broadcast!(:pets, %{owner_id: id}, :rehomed, %{id: "c3"})

    refute_receive {:adopted, _}, 50
    refute_receive {:renamed, _, _}, 50
    assert Process.alive?(pid)
  end

  @tag :capture_log
  test "an unrelated message raises when the module does not handle it", %{worker: pid} do
    # A module that defines handle_info/2 loses the default clause that `use GenServer`
    # installs. Elixir 1.20 made super/2 for GenServer callbacks an error, so the module
    # cannot keep that default. Any GenServer with its own handle_info/2 does the same. A
    # subscriber that receives other messages adds a catch-all.
    ref = Process.monitor(pid)
    send(pid, :something_unrelated)

    # The exit reason is the raw Erlang form, not a %FunctionClauseError{} struct.
    # A GenServer that crashes logs its crash report before it exits. So :DOWN can take
    # longer than the default 100ms on a busy CI runner.
    assert_receive {:DOWN, ^ref, :process, ^pid, {:function_clause, _stacktrace}}, 1_000
  end

  test "a catch-all of the module handles unrelated messages, and hides no generated clause" do
    id = unique_owner_id()
    pid = start_supervised!({WorkerWithCatchAll, owner: self(), owner_id: id})

    send(pid, :something_unrelated)
    assert_receive {:catchall, :something_unrelated}
    assert Process.alive?(pid)

    # The `use` puts the generated clause before the catch-all of the user. So Elixir tries
    # the generated clause first, and verified messages still reach their handlers.
    broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "c7"})
    assert_receive {:adopted, %{id: "c7"}}
  end

  test "works next to a handle_info clause that the user defines" do
    id = unique_owner_id()
    pid = start_supervised!({WorkerWithOwnHandleInfo, owner: self(), owner_id: id})

    send(pid, :tick)
    assert_receive :ticked

    broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "c9"})
    assert_receive {:adopted, %{id: "c9"}}

    assert Process.alive?(pid)
  end
end
