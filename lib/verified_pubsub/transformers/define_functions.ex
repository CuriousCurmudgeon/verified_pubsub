defmodule VerifiedPubsub.Transformers.DefineFunctions do
  @moduledoc """
  Generates the `topic_*`, `subscribe_*`, `unsubscribe_*`, and `broadcast_*` functions
  onto the registry module.

  Because an undeclared topic or event simply has no generated function, calling one is
  an ordinary undefined-function compile error — the same mechanism verified routes
  relies on.
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def after?(VerifiedPubsub.Transformers.ParseParams), do: true
  def after?(VerifiedPubsub.Transformers.ValidateTopics), do: true
  def after?(_), do: false

  @impl true
  def transform(dsl) do
    dsl
    |> Transformer.get_entities([:topics])
    |> Enum.reduce({:ok, dsl}, fn topic, {:ok, acc} ->
      {:ok, Transformer.eval(acc, [], topic_functions(topic))}
    end)
  end

  # The params argument is a map pattern destructuring exactly the topic's params, so a
  # missing key raises FunctionClauseError. For a param-free topic it is omitted.
  defp topic_functions(topic) do
    name = topic.name
    params = topic.params

    args = params_args(params)
    interpolation = interpolation_ast(topic.pattern, params)
    params_map = params_map_ast(params)

    topic_fn = :"topic_#{name}"
    subscribe_fn = :"subscribe_#{name}"
    unsubscribe_fn = :"unsubscribe_#{name}"

    broadcasts =
      Enum.map(topic.messages, fn message ->
        broadcast_functions(name, message.name, args, params_map, topic_fn)
      end)

    quote do
      @doc "Returns the wire topic string for `#{unquote(inspect(name))}`."
      def unquote(topic_fn)(unquote_splicing(args)) do
        unquote(interpolation)
      end

      @doc "Subscribes the calling process to `#{unquote(inspect(name))}`."
      def unquote(subscribe_fn)(unquote_splicing(args)) do
        __verified_pubsub_adapter__().subscribe(
          __verified_pubsub_config__(),
          unquote(topic_fn)(unquote_splicing(args))
        )
      end

      @doc "Unsubscribes the calling process from `#{unquote(inspect(name))}`."
      def unquote(unsubscribe_fn)(unquote_splicing(args)) do
        __verified_pubsub_adapter__().unsubscribe(
          __verified_pubsub_config__(),
          unquote(topic_fn)(unquote_splicing(args))
        )
      end

      unquote_splicing(broadcasts)
    end
  end

  defp broadcast_functions(topic_name, event, args, params_map, topic_fn) do
    fn_name = :"broadcast_#{topic_name}_#{event}"
    bang_name = :"broadcast_#{topic_name}_#{event}!"
    payload = Macro.var(:payload, __MODULE__)
    all_args = args ++ [payload]

    quote do
      @doc "Broadcasts `#{unquote(inspect(event))}` on `#{unquote(inspect(topic_name))}`."
      def unquote(fn_name)(unquote_splicing(all_args)) do
        message = %VerifiedPubsub.Message{
          registry: __MODULE__,
          topic: unquote(topic_name),
          event: unquote(event),
          params: unquote(params_map),
          payload: unquote(payload)
        }

        __verified_pubsub_adapter__().broadcast(
          __verified_pubsub_config__(),
          unquote(topic_fn)(unquote_splicing(args)),
          message
        )
      end

      @doc "Same as `#{unquote(fn_name)}/#{unquote(length(all_args))}` but raises on failure."
      def unquote(bang_name)(unquote_splicing(all_args)) do
        VerifiedPubsub.Broadcast.bang!(
          unquote(fn_name)(unquote_splicing(all_args)),
          unquote(topic_name),
          unquote(event)
        )
      end
    end
  end

  defp params_args([]), do: []
  defp params_args(params), do: [params_map_ast(params)]

  defp params_map_ast(params) do
    {:%{}, [], Enum.map(params, fn p -> {p, Macro.var(p, __MODULE__)} end)}
  end

  # Turns "accounts:%{account_id}:campaigns" into the AST for
  # "accounts:" <> to_string(account_id) <> ":campaigns".
  defp interpolation_ast(pattern, params) do
    literals = String.split(pattern, ~r/%\{[^}]*\}/)

    params
    |> Enum.zip(tl(literals))
    |> Enum.reduce(hd(literals), fn {param, literal}, acc ->
      quote do
        unquote(acc) <> to_string(unquote(Macro.var(param, __MODULE__))) <> unquote(literal)
      end
    end)
  end
end
