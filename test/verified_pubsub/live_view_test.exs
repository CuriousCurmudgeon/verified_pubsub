defmodule VerifiedPubSub.LiveViewTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestManifests.Basic

  import VerifiedPubSub.CompileHelper

  defmodule PetsLive do
    use Phoenix.LiveView

    use VerifiedPubSub.Subscriber,
      manifest: VerifiedPubSub.TestManifests.Basic

    @impl true
    def mount(_params, _session, socket) do
      {:ok, assign(socket, :pets, [])}
    end

    @impl true
    def render(assigns), do: ~H"<div>{length(@pets)}</div>"

    # `use VerifiedPubSub.Subscriber` imports VerifiedPubSub.Api, so the macros are
    # usable here without a second `use`.
    def subscribe_to(owner_id), do: subscribe(:pets, %{owner_id: owner_id})

    def announce(owner_id, payload) do
      broadcast!(:pets, %{owner_id: owner_id}, :adopted, payload)
    end

    handle_message :pets, :adopted, payload, socket do
      {:noreply, assign(socket, :pets, [payload | socket.assigns.pets])}
    end

    ignore_message :pets, :renamed
    ignore_message :pets, :rehomed
  end

  defp message(event, payload) do
    %VerifiedPubSub.Message{
      manifest: Basic,
      topic: :pets,
      event: event,
      params: %{owner_id: "7"},
      payload: payload
    }
  end

  test "the generated handle_info runs inside a LiveView" do
    {:ok, socket} = PetsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})

    assert {:noreply, updated} =
             PetsLive.handle_info(message(:adopted, %{id: "c1"}), socket)

    assert updated.assigns.pets == [%{id: "c1"}]
  end

  test "an ignored event leaves the socket untouched" do
    {:ok, socket} = PetsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})

    assert {:noreply, ^socket} =
             PetsLive.handle_info(message(:rehomed, %{id: "c1"}), socket)
  end

  test "the API macros are imported into the LiveView, and round-trip a message" do
    owner_id = unique_owner_id()

    assert :ok = PetsLive.subscribe_to(owner_id)
    assert :ok = PetsLive.announce(owner_id, %{id: "c1"})

    assert_receive %Message{topic: :pets, event: :adopted, payload: %{id: "c1"}} = message

    {:ok, socket} = PetsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})
    assert {:noreply, updated} = PetsLive.handle_info(message, socket)
    assert updated.assigns.pets == [%{id: "c1"}]
  end
end
