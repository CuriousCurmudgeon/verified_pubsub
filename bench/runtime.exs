# Runtime cost of VerifiedPubSub over plain Phoenix.PubSub.
#
#     mix run bench/runtime.exs
#
# Each scenario does the same work in two ways. The plain side builds the wire topic by
# hand and sends a `{event, payload}` tuple. The verified side uses the macros, so it also
# validates the params and the payload, and puts them in a VerifiedPubSub.Message.

{:ok, _} = Phoenix.PubSub.Supervisor.start_link(name: Bench.PubSub)

defmodule Bench.Topics do
  use VerifiedPubSub.Manifest, pubsub: Bench.PubSub

  topic :pets, ["owners", :owner_id, "pets"] do
    message :adopted do
      field :id, :string
      field :name, :string
    end
  end

  topic :wide, ["wide", :id] do
    message :changed do
      field :f1, :string
      field :f2, :string
      field :f3, :string
      field :f4, :string
      field :f5, :integer
      field :f6, :integer
      field :f7, :integer
      field :f8, :integer
      field :f9, :boolean
      field :f10, :boolean
      field :f11, :atom
      field :f12, :atom
      field :f13, :map
      field :f14, :map
      field :f15, :float
      field :f16, :float
      field :f17, :any
      field :f18, :any
      field :f19, :string, required: false
      field :f20, :string, required: false
    end
  end

  topic :tagged, ["tagged", :id] do
    message :tags do
      field :tags, {:list, :string}
    end
  end
end

defmodule Bench.Plain do
  def pets(owner_id, payload),
    do:
      Phoenix.PubSub.broadcast(
        Bench.PubSub,
        "owners:" <> owner_id <> ":pets",
        {:adopted, payload}
      )

  def wide(id, payload),
    do: Phoenix.PubSub.broadcast(Bench.PubSub, "wide:" <> id, {:changed, payload})

  def tagged(id, payload),
    do: Phoenix.PubSub.broadcast(Bench.PubSub, "tagged:" <> id, {:tags, payload})

  def subscribe(owner_id),
    do: Phoenix.PubSub.subscribe(Bench.PubSub, "owners:" <> owner_id <> ":pets")

  def handle_info({:adopted, %{id: _, name: _}}, state), do: {:noreply, state}
end

defmodule Bench.Verified do
  use VerifiedPubSub, manifest: Bench.Topics

  def pets(owner_id, payload), do: broadcast(:pets, %{owner_id: owner_id}, :adopted, payload)
  def wide(id, payload), do: broadcast(:wide, %{id: id}, :changed, payload)
  def tagged(id, payload), do: broadcast(:tagged, %{id: id}, :tags, payload)
  def subscribe(owner_id), do: subscribe(:pets, %{owner_id: owner_id})
end

defmodule Bench.Subscriber do
  use VerifiedPubSub.Subscriber, manifest: Bench.Topics

  handle_message :pets, :adopted, %{id: _, name: _}, state do
    {:noreply, state}
  end
end

small = %{id: "p1", name: "Biscuit"}

wide = %{
  f1: "a",
  f2: "b",
  f3: "c",
  f4: "d",
  f5: 1,
  f6: 2,
  f7: 3,
  f8: 4,
  f9: true,
  f10: false,
  f11: :x,
  f12: :y,
  f13: %{},
  f14: %{a: 1},
  f15: 1.0,
  f16: 2.0,
  f17: nil,
  f18: [1, 2],
  f19: "e",
  f20: nil
}

tags = fn n -> %{tags: Enum.map(1..n, &"tag#{&1}")} end

benchee_opts = [warmup: 1, time: 3, memory_time: 1, print: [configuration: false]]

section = fn title ->
  IO.puts("\n\n#{String.duplicate("=", 78)}\n#{title}\n#{String.duplicate("=", 78)}")
end

# -- 1. Broadcast with no subscribers --------------------------------------------------
# This is the worst case for relative overhead. The PubSub work is only a registry
# lookup, so the validation is a large part of the total.

section.("1. broadcast, no subscribers")

Benchee.run(
  %{
    "plain    small (2 fields)" => fn -> Bench.Plain.pets("o1", small) end,
    "verified small (2 fields)" => fn -> Bench.Verified.pets("o1", small) end,
    "plain    wide (20 fields)" => fn -> Bench.Plain.wide("w1", wide) end,
    "verified wide (20 fields)" => fn -> Bench.Verified.wide("w1", wide) end
  },
  benchee_opts
)

# -- 2. {:list, type} fields -------------------------------------------------------------
# A {:list, inner} field checks every element. So this validation is the only one that
# grows with the payload, not with the declaration.

section.("2. broadcast, no subscribers, {:list, :string} field of N elements")

Benchee.run(
  %{
    "plain" => fn payload -> Bench.Plain.tagged("t1", payload) end,
    "verified" => fn payload -> Bench.Verified.tagged("t1", payload) end
  },
  Keyword.put(benchee_opts, :inputs, [
    {"10 tags", tags.(10)},
    {"1_000 tags", tags.(1_000)},
    {"100_000 tags", tags.(100_000)}
  ])
)

# -- 3. Round trip through one subscriber ----------------------------------------------
# Broadcast to the benchmark process, receive the message, and run it through
# handle_info/2. This is the full cost of each message for a LiveView or a GenServer,
# minus the scheduler hop.

section.("3. round trip: broadcast -> receive -> handle_info/2, one subscriber")

ensure_subscribed = fn key, subscribe ->
  unless Process.get(key) do
    :ok = subscribe.()
    Process.put(key, true)
  end
end

Benchee.run(
  %{
    "plain" => fn ->
      ensure_subscribed.(:plain, fn -> Bench.Plain.subscribe("rt-plain") end)
      :ok = Bench.Plain.pets("rt-plain", small)

      receive do
        msg -> Bench.Plain.handle_info(msg, nil)
      end
    end,
    "verified" => fn ->
      ensure_subscribed.(:verified, fn -> Bench.Verified.subscribe("rt-verified") end)
      :ok = Bench.Verified.pets("rt-verified", small)

      receive do
        msg -> Bench.Subscriber.handle_info(msg, nil)
      end
    end
  },
  benchee_opts
)

# -- 4. Fan-out ---------------------------------------------------------------------------
# Validation runs once for each broadcast. But the BEAM copies the larger Message struct
# into the mailbox of each subscribed process.

section.("4. broadcast to N subscribers (broadcaster's cost)")

drain = fn drain ->
  receive do
    _ -> drain.(drain)
  end
end

for {owner, subscribe} <- [
      {"fan-plain", &Bench.Plain.subscribe/1},
      {"fan-verified", &Bench.Verified.subscribe/1}
    ],
    _ <- 1..100 do
  parent = self()

  spawn(fn ->
    :ok = subscribe.(owner)
    send(parent, :subscribed)
    drain.(drain)
  end)

  receive do
    :subscribed -> :ok
  end
end

Benchee.run(
  %{
    "plain    100 subscribers" => fn -> Bench.Plain.pets("fan-plain", small) end,
    "verified 100 subscribers" => fn -> Bench.Verified.pets("fan-verified", small) end
  },
  benchee_opts
)

# -- 5. Message size ----------------------------------------------------------------------
# What the mailbox of each subscribed process receives, and what goes over the wire to
# another node.

section.("5. message size")

plain_msg = {:adopted, small}

verified_msg = %VerifiedPubSub.Message{
  manifest: Bench.Topics,
  topic: :pets,
  event: :adopted,
  params: %{owner_id: "o1"},
  payload: small
}

for {label, msg} <- [{"plain   ", plain_msg}, {"verified", verified_msg}] do
  IO.puts(
    "#{label}  heap words: #{:erts_debug.flat_size(msg)}  " <>
      "term_to_binary bytes: #{byte_size(:erlang.term_to_binary(msg))}"
  )
end
