defmodule VerifiedPubSub do
  @moduledoc """
  Compile-time verified PubSub.

  Usually, a broadcast and its handler are two string literals in two files, and nothing
  connects them. When you rename or delete an event, dead handlers and unhandled messages
  stay behind, and nothing reports them. `VerifiedPubSub` makes a manifest the one place
  that declares each topic and event. A change that breaks a call site is then a compile
  error.

  ## The manifest

      defmodule MyApp.Topics do
        use VerifiedPubSub.Manifest, pubsub: MyApp.PubSub

        topic :pets, ["owners", :owner_id, "pets"] do
          message :adopted do
            field :id, :string
            field :name, :string
          end

          message :rehomed do
            field :id, :string
          end
        end
      end

  `:pets` is the name that call sites use. The list is the wire topic, so a change to its
  segments does not change a call site. The segments are joined with `:`. A string is a
  literal and an atom is a param. The params come from the segments, so you do not declare
  them a second time.

  ## Broadcasting

      defmodule MyApp.Pets do
        use VerifiedPubSub, manifest: MyApp.Topics

        def create(attrs) do
          # ...
          broadcast!(:pets, %{owner_id: attrs.owner_id}, :adopted, payload)
        end
      end

  `use VerifiedPubSub, manifest: ...` imports `VerifiedPubSub.Api`. The topic and the
  event are normal arguments, but these are macros, so they must be **literal atoms**.
  Because of this, a typo is a compile error. Params can be built at runtime.

  A topic with no params takes no params argument, as in
  `broadcast!(:system, :alert, payload)` and `subscribe(:system)`. An explicit `%{}` also
  works. When a topic takes params and the call site omits them, the compile error names
  them.

  A topic is always followed immediately by its params. The params only fill in the topic
  segments, so `{topic, params}` is the address and `{event, payload}` is the message.
  `Phoenix.PubSub.broadcast/3` uses the same order: first the address, then the message.

  To skip the sender, use `broadcast_from!/5`. It works like
  `Phoenix.PubSub.broadcast_from/4`, and `from` comes first, as it does there:

      broadcast_from!(self(), :pets, %{owner_id: id}, :adopted, payload)

  ## Subscribing

      defmodule MyApp.Worker do
        use GenServer
        use VerifiedPubSub.Subscriber, manifest: MyApp.Topics

        def init(owner_id) do
          :ok = subscribe(:pets, %{owner_id: owner_id})
          {:ok, owner_id}
        end

        handle_message :pets, :adopted, payload, state do
          {:noreply, state}
        end

        ignore_message :pets, :rehomed
      end

  A subscriber must handle or explicitly ignore each event that a subscribed topic
  declares. Subscribers often need only some of the events of a topic, so
  `ignore_message/2` exists. Without it, the coverage check would be too strict to use.

  To match on topic params, put them where `broadcast!/4` takes them: right after the
  topic. The leading arguments are the same in both. A broadcast builds them, and a
  handler matches them:

      broadcast!     :pets, %{owner_id: id},    :adopted, payload
      handle_message :pets, %{owner_id: owner}, :adopted, payload, state

  The params argument is optional, and a pattern can name a subset of the params. A
  pattern can also match the whole `%VerifiedPubSub.Message{}` in the payload position, to
  get the other data that the message carries.

  ## What is and is not checked

  Each of these is a compile error:

    * a subscriber that does not handle or ignore each event of a topic that it
      subscribes to
    * a subscriber that handles an event that the manifest does not declare, or names a
      topic that the manifest does not declare
    * duplicate topics, duplicate events on one topic, and topic segments that are not
      valid
    * two topics whose segments can match the same wire topic

    * a broadcast to an unknown topic, with an unknown event, or with an event of a
      different topic
    * a literal params map with missing or unexpected keys

  The compiler cannot check a params map built at runtime. Instead, `Map.fetch!/2` raises
  `KeyError` for a missing key.

  The library also checks payload shapes. It checks a **literal** payload map at compile
  time. It checks a payload built at runtime on every broadcast, in every environment.
  Both `broadcast/4` and `broadcast!/4` raise `VerifiedPubSub.PayloadError`. See "Payload
  shapes" below.

  Not checked:

    * **Topic param values.** The coverage check counts each `{topic, event}` pair. If
      every clause for an event matches only some param values, the event still counts
      as covered. A message with a different value then raises `FunctionClauseError`.
      When clauses match on param values, end with a clause that matches any value.

  ## Payload shapes

  Each `field` declares a key that the payload must carry:

      message :adopted do
        field :id, :string
        field :pet, MyApp.Pet
        field :tags, {:list, :string}
        field :note, :string, required: false
      end

  A type is one of `:string`, `:integer`, `:float`, `:boolean`, `:atom`, `:map`, `:list`,
  `:any`, a `{:list, type}` tuple, or a struct module. An unknown type is a compile error.

  **The payload is a map of exactly the declared fields.** The library rejects undeclared
  keys, so the manifest stays an accurate description of the data on the wire. For the
  same reason, a struct cannot be the payload: it carries `__struct__` and all of its own
  keys. Put the struct in a field instead:

      broadcast!(:pets, %{owner_id: id}, :adopted, %{pet: pet})

  `required: false` lets the key be absent, or present as `nil`.

  ## Topic segments

  A topic is a list of segments joined with `:`, so each param always fills a whole
  segment. A literal segment cannot be empty or contain `:`. Each call checks the param
  values. A value must not be empty and must not contain `:`, or the call raises
  `VerifiedPubSub.TopicError`. The manifest also makes sure that no two topics match the
  same wire topic. Together, these checks make sure that a wire topic built from params is
  always the topic that its call site names. See `VerifiedPubSub.Topic`.

  ## Docs

  With `docs: true`, a module gets a `## PubSub` section at the end of its `@moduledoc`.
  The section lists each topic that the module broadcasts to, with its events. It also
  lists each topic of a `subscribe/2` call, with no events, because the module handles
  the messages itself:

      use VerifiedPubSub, manifest: MyApp.Topics, docs: true

  ```markdown
  ## PubSub

    * Publishes to `"owners:<owner_id>:pets"` for `:adopted` and `:rehomed`.
    * Subscribes to `"system"`.
  ```

  `VerifiedPubSub.Subscriber` lists the events that its handlers receive.

  The macro calls of the module give the section, so the section agrees with the code. It
  shows only the calls in the module itself. `@moduledoc false` keeps the module hidden.

  ## Transport

  Broadcasts and subscriptions go through `Phoenix.PubSub`, so `:pubsub` names a
  `Phoenix.PubSub` that your supervision tree starts:

      children = [{Phoenix.PubSub, name: MyApp.PubSub}]

  This library has no adapter layer, on purpose. `Phoenix.PubSub` already has an adapter
  behaviour, and you configure PG2, Redis, and other transports there. A wrapper here
  would repeat an extension point that the layer below already has. You would then
  configure the transport in two places.
  """

  @doc """
  Imports the atom-first macros in `VerifiedPubSub.Api`, bound to `manifest`.
  """
  defmacro __using__(opts) do
    manifest =
      opts
      |> Keyword.fetch!(:manifest)
      |> Macro.expand(__CALLER__)

    bind_manifest!(__CALLER__.module, manifest)
    bind_docs!(__CALLER__.module, Keyword.get(opts, :docs))

    quote do
      import VerifiedPubSub.Api

      @before_compile VerifiedPubSub
    end
  end

  @doc false
  # The section must see each macro call of the module, so the moduledoc is written by code
  # that this hook returns. A module can register this hook more than once, so the write
  # can run more than once.
  defmacro __before_compile__(env) do
    quote do
      VerifiedPubSub.Docs.put_moduledoc(__MODULE__, unquote(env.line))
    end
  end

  @doc false
  # Binds `module` to `manifest`. The macros read this binding to find topics and events.
  # A module binds exactly one manifest. A second bind to the same manifest does nothing,
  # and a bind to another manifest raises. Each `use` that binds a manifest calls this
  # function, so one `use` cannot silently replace the manifest that another `use` bound.
  #
  # This runs at macro expansion, not inside a quote. Elixir expands the macros in a module
  # body before the runtime calls of that body run. A `broadcast!` lower in the same module
  # would not see an assignment in the quote.
  @spec bind_manifest!(module(), module()) :: :ok
  def bind_manifest!(module, manifest) do
    case Module.get_attribute(module, :verified_pubsub_manifest) do
      nil ->
        Module.put_attribute(module, :verified_pubsub_manifest, manifest)
        Module.register_attribute(module, :verified_pubsub_broadcasts, accumulate: true)
        Module.register_attribute(module, :verified_pubsub_subscriptions, accumulate: true)

      ^manifest ->
        :ok

      other ->
        raise ArgumentError,
              "#{inspect(module)} is already bound to manifest #{inspect(other)}, " <>
                "cannot also bind #{inspect(manifest)}"
    end
  end

  @doc false
  # Records the `docs` option of one `use` line. `nil` means that the line gives no value.
  # A module with two `use` lines takes one value, so two different values raise.
  @spec bind_docs!(module(), boolean() | nil) :: :ok
  def bind_docs!(_module, nil), do: :ok

  def bind_docs!(module, docs) when is_boolean(docs) do
    case Module.get_attribute(module, :verified_pubsub_docs) do
      nil ->
        Module.put_attribute(module, :verified_pubsub_docs, docs)

      ^docs ->
        :ok

      _other ->
        raise ArgumentError,
              "#{inspect(module)} sets both docs: true and docs: false. Give one value."
    end
  end

  def bind_docs!(_module, docs) do
    raise ArgumentError, "expected a boolean for :docs, got: #{inspect(docs)}"
  end
end
