defmodule VerifiedPubsub do
  @moduledoc """
  Defines macros for verified PubSub. This provides compile-time support for PubSub
  so you know that you are using valid topics and handling all possible messages on that topic.

  ## Examples
  ```
  topic "campaigns" do
    message :created, %{id: :string, account_id: :string, name: String.t()}},
    message :updated, %{id: :string, account_id: :string, name: String.t()}},
    message :deleted, %{id: :string, account_id: :string}}
  end
  ```
  """

  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :topics, accumulate: true)

      import unquote(__MODULE__), only: [topic: 1, message: 2]
      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(env) do
    compile(Module.get_attribute(env.module, :topics))
  end

  defmacro topic(name, fun) do
    quote bind_quoted: [name: name, fun: fun] do
      @topics {name, fun}
    end
  end

  defmacro message(name, schema) do
    quote bind_quoted: [name: name, schema: schema] do
      @messages {name, schema}
    end
  end

  def compile(_topics) do
  end
end
