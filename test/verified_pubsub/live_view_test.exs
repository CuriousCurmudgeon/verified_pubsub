defmodule VerifiedPubSub.LiveViewTest do
  use ExUnit.Case, async: true

  import VerifiedPubSub.CompileHelper

  alias VerifiedPubSub.Message
  alias VerifiedPubSub.TestManifests.Basic

  defmodule CampaignsLive do
    use Phoenix.LiveView

    use VerifiedPubSub.Subscriber,
      manifest: VerifiedPubSub.TestManifests.Basic

    @impl true
    def mount(_params, _session, socket) do
      {:ok, assign(socket, :campaigns, [])}
    end

    @impl true
    def render(assigns), do: ~H"<div>{length(@campaigns)}</div>"

    # `use VerifiedPubSub.Subscriber` imports VerifiedPubSub.Api, so the macros are
    # usable here without a second `use`.
    def subscribe_to(account_id), do: subscribe(:campaigns, %{account_id: account_id})

    def announce(account_id, payload) do
      broadcast!(:campaigns, %{account_id: account_id}, :created, payload)
    end

    handle_message :campaigns, :created, payload, socket do
      {:noreply, assign(socket, :campaigns, [payload | socket.assigns.campaigns])}
    end

    ignore_message :campaigns, :updated
    ignore_message :campaigns, :deleted
  end

  defp message(event, payload) do
    %VerifiedPubSub.Message{
      manifest: Basic,
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

  test "the API macros are imported into the LiveView, and round-trip a message" do
    account_id = unique_account_id()

    assert :ok = CampaignsLive.subscribe_to(account_id)
    assert :ok = CampaignsLive.announce(account_id, %{id: "c1"})

    assert_receive %Message{topic: :campaigns, event: :created, payload: %{id: "c1"}} = message

    {:ok, socket} = CampaignsLive.mount(%{}, %{}, %Phoenix.LiveView.Socket{})
    assert {:noreply, updated} = CampaignsLive.handle_info(message, socket)
    assert updated.assigns.campaigns == [%{id: "c1"}]
  end
end
