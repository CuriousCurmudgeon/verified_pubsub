defmodule VerifiedPubSub.Usage.Entry do
  @moduledoc false

  @typedoc """
  The kind of call that records an entry:

  * `:broadcast` - `broadcast/3`, `broadcast/4`, `broadcast!/3`, or `broadcast!/4`
  * `:broadcast_from` - `broadcast_from/4`, `broadcast_from/5`, `broadcast_from!/4`, or
    `broadcast_from!/5`
  * `:handle` - `handle_broadcast/5` or `handle_broadcast/6`
  * `:ignore` - `ignore_broadcast/2`
  * `:subscribe` - `subscribe/1` or `subscribe/2`
  * `:unsubscribe` - `unsubscribe/1` or `unsubscribe/2`
  """
  @type kind :: :broadcast | :broadcast_from | :handle | :ignore | :subscribe | :unsubscribe

  @typedoc """
  One call site of a topic:

  * `:manifest` - the manifest that the module binds
  * `:topic` - the name of the topic
  * `:event` - the name of the event, or `nil` for a call that takes no event
  * `:kind` - the kind of call
  * `:module` - the module that makes the call
  * `:file` - the source file of the call
  * `:line` - the line of the call
  """
  @type t :: %__MODULE__{
          manifest: module(),
          topic: atom(),
          event: atom() | nil,
          kind: kind(),
          module: module(),
          file: String.t(),
          line: pos_integer()
        }

  @enforce_keys [:manifest, :topic, :event, :kind, :module, :file, :line]
  defstruct @enforce_keys
end
