defmodule VerifiedPubSub.ManifestIsolationTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.ManifestMismatchError
  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestManifests.CollideA
  alias VerifiedPubSub.TestManifests.CollideB

  defmodule Listener do
    use GenServer
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.CollideA

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
  end

  defmodule ListenerWithCatchAll do
    use GenServer
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.CollideA

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

    @impl true
    def handle_info(other, state) do
      send(state.owner, {:catchall, other})
      {:noreply, state}
    end
  end

  defmodule Publisher do
    use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.CollideB

    def announce(id), do: broadcast!(:pets, %{owner_id: id}, :adopted, %{species: :cat})
  end

  defp message(manifest, payload) do
    %Message{
      manifest: manifest,
      topic: :pets,
      event: :adopted,
      params: %{owner_id: "7"},
      payload: payload
    }
  end

  describe "a message from another manifest" do
    test "goes to no handler, and the error names both manifests" do
      error =
        assert_raise ManifestMismatchError, fn ->
          Listener.handle_info(message(CollideB, %{species: :cat}), %{owner: self()})
        end

      text = Exception.message(error)
      assert text =~ inspect(CollideA)
      assert text =~ inspect(CollideB)
      assert text =~ inspect(Listener)
      assert text =~ ":pets"
      assert text =~ ":adopted"
    end

    test "names the wire topic, and the topic of the subscriber that has the same wire topic" do
      error =
        assert_raise ManifestMismatchError, fn ->
          Listener.handle_info(message(CollideB, %{species: :cat}), %{owner: self()})
        end

      text = Exception.message(error)
      assert text =~ ~s|wire topic "collide:7:pets"|
      assert text =~ "matches :pets (collide:<owner_id>:pets) in #{inspect(CollideA)}"
    end

    test "does not reach the handle_info clauses of the module" do
      # The docs recommend a catch-all for other messages. If the catch-all takes a message
      # from another manifest, nothing ever shows the collision.
      assert_raise ManifestMismatchError, fn ->
        ListenerWithCatchAll.handle_info(message(CollideB, %{species: :dog}), %{owner: self()})
      end
    end

    test "never runs the handler, so a payload from another manifest cannot reach it" do
      assert_raise ManifestMismatchError, fn ->
        Listener.handle_info(message(CollideB, %{species: :cat}), %{owner: self()})
      end

      refute_receive {:adopted, _}, 20
    end
  end

  describe "a message that is not from another manifest" do
    test "a message from the bound manifest goes to its handler" do
      assert {:noreply, _} =
               Listener.handle_info(message(CollideA, %{id: "c1"}), %{owner: self()})

      assert_receive {:adopted, %{id: "c1"}}
    end

    test "a message that is not a VerifiedPubSub.Message still reaches the catch-all" do
      assert {:noreply, _} = ListenerWithCatchAll.handle_info(:tick, %{owner: self()})
      assert_receive {:catchall, :tick}
    end
  end

  describe "a running subscriber" do
    @tag :capture_log
    test "a broadcast on the same wire topic from another manifest crashes the subscriber" do
      id = "iso-#{System.unique_integer([:positive])}"
      pid = start_supervised!({Listener, owner: self(), owner_id: id})
      ref = Process.monitor(pid)

      # The wire topics are the same, so Phoenix.PubSub delivers the message.
      assert VerifiedPubSub.Info.topic!(CollideA, :pets).segments ==
               VerifiedPubSub.Info.topic!(CollideB, :pets).segments

      assert :ok = Publisher.announce(id)

      # A GenServer that crashes logs its crash report before it exits. So :DOWN can take
      # longer than the default 100ms on a busy CI runner.
      assert_receive {:DOWN, ^ref, :process, ^pid, {%ManifestMismatchError{}, _stacktrace}},
                     1_000

      refute_receive {:adopted, _}, 20
    end
  end
end
