defmodule VerifiedPubSub.LiveViewTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.TestRegistries.Basic

  defmodule CampaignsLive do
    use Phoenix.LiveView

    use VerifiedPubSub.Subscriber,
      registry: VerifiedPubSub.TestRegistries.Basic,
      topics: [:campaigns]

    @impl true
    def mount(_params, _session, socket) do
      {:ok, assign(socket, :campaigns, [])}
    end

    @impl true
    def render(assigns), do: ~H"<div>{length(@campaigns)}</div>"

    handle_message :campaigns, :created, payload, socket do
      {:noreply, assign(socket, :campaigns, [payload | socket.assigns.campaigns])}
    end

    ignore_message :campaigns, :updated
    ignore_message :campaigns, :deleted
  end

  defp message(event, payload) do
    %VerifiedPubSub.Message{
      registry: Basic,
      topic: :campaigns,
      event: event,
      params: %{account_id: "7"},
      payload: payload
    }
  end

  test "the generated handle_info runs inside a LiveView" do
    {:ok, socket} = CampaignsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})

    assert {:noreply, updated} =
             CampaignsLive.handle_info(message(:created, %{id: "c1"}), socket)

    assert updated.assigns.campaigns == [%{id: "c1"}]
  end

  test "an ignored event leaves the socket untouched" do
    {:ok, socket} = CampaignsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})

    assert {:noreply, ^socket} =
             CampaignsLive.handle_info(message(:deleted, %{id: "c1"}), socket)
  end

  test "subscribe_* is imported into the LiveView" do
    assert function_exported?(CampaignsLive, :handle_info, 2)
    assert function_exported?(CampaignsLive, :mount, 3)
  end
end
