defmodule VerifiedPubSub.Usage do
  @moduledoc false

  # Records each call site of a topic in the module that makes the call. `record!/5` runs at
  # expansion, and the entries persist in the `.beam` of the module.

  alias VerifiedPubSub.Usage.Entry

  @attribute :verified_pubsub_usage

  @doc false
  # Must run at expansion, not in a quote, for the reason that the comment in
  # `VerifiedPubSub.Subscriber.__using__/1` gives.
  @spec register!(module()) :: :ok
  def register!(module) do
    Module.register_attribute(module, @attribute, accumulate: true, persist: true)
  end

  @doc false
  @spec record!(Macro.Env.t(), module(), atom(), atom() | nil, Entry.kind()) :: :ok
  def record!(caller, manifest, topic, event, kind) do
    Module.put_attribute(caller.module, @attribute, %Entry{
      manifest: manifest,
      topic: topic,
      event: event,
      kind: kind,
      module: caller.module,
      file: caller.file,
      line: caller.line
    })
  end

  @doc false
  @spec entries(module()) :: [Entry.t()]
  # The entries follow the order of expansion, not the order of the source. Elixir expands
  # the body of a `def` after the calls at the top level of the module.
  #
  # The module list of an `.app` file can name a module that no longer exists, after a
  # source file is deleted, so a module that does not load has no entries. Only an Elixir
  # module defines `__info__/1`, so the function reads `module_info/1`, which each BEAM
  # module defines.
  def entries(module) do
    if Code.ensure_loaded?(module) do
      module.module_info(:attributes)
      |> Keyword.get_values(@attribute)
      |> List.flatten()
    else
      []
    end
  end
end
