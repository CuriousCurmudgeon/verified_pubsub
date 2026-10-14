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
    test "is not dispatched, and the error names both manifests" do
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

    test "names the wire topic and the subscriber's topic it collided with" do
      error =
        assert_raise ManifestMismatchError, fn ->
          Listener.handle_info(message(CollideB, %{species: :cat}), %{owner: self()})
        end

      text = Exception.message(error)
      assert text =~ ~s|wire topic "collide:7:pets"|
      assert text =~ "matches :pets (collide:<owner_id>:pets) in #{inspect(CollideA)}"
    end

    test "does not reach the module's own handle_info clauses" do
      # A catch-all is what the docs recommend for other messages. It must not swallow a
      # cross-manifest message, or the collision goes unnoticed forever.
      assert_raise ManifestMismatchError, fn ->
        ListenerWithCatchAll.handle_info(message(CollideB, %{species: :dog}), %{owner: self()})
      end
    end

    test "never runs the handler, so a foreign payload cannot reach it" do
      assert_raise ManifestMismatchError, fn ->
        Listener.handle_info(message(CollideB, %{species: :cat}), %{owner: self()})
      end

      refute_receive {:adopted, _}, 20
    end
  end

  describe "the bound manifest still works" do
    test "a message from the bound manifest dispatches" do
      assert {:noreply, _} =
               Listener.handle_info(message(CollideA, %{id: "c1"}), %{owner: self()})

      assert_receive {:adopted, %{id: "c1"}}
    end

    test "a non-Message message still reaches the module's catch-all" do
      assert {:noreply, _} = ListenerWithCatchAll.handle_info(:tick, %{owner: self()})
      assert_receive {:catchall, :tick}
    end
  end

  describe "end to end, over the wire" do
    @tag :capture_log
    test "a colliding broadcast from another manifest crashes the subscriber" do
      id = "iso-#{System.unique_integer([:positive])}"
      pid = start_supervised!({Listener, owner: self(), owner_id: id})
      ref = Process.monitor(pid)

      # Identical wire topics, so Phoenix.PubSub really does deliver it.
      assert VerifiedPubSub.Info.topic!(CollideA, :pets).segments ==
               VerifiedPubSub.Info.topic!(CollideB, :pets).segments

      assert :ok = Publisher.announce(id)

      # A crashing GenServer logs its crash report before it exits, so :DOWN can take
      # longer than the default 100ms on a loaded CI runner.
      assert_receive {:DOWN, ^ref, :process, ^pid, {%ManifestMismatchError{}, _stacktrace}},
                     1_000

      refute_receive {:adopted, _}, 20
    end
  end
end
