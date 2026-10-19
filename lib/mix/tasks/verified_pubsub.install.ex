if Code.ensure_loaded?(Igniter) do
  defmodule Mix.Tasks.VerifiedPubsub.Install do
    @shortdoc "Installs VerifiedPubSub into a project"

    @moduledoc """
    #{@shortdoc}.

        mix igniter.install verified_pubsub

    Adds `:verified_pubsub` to `import_deps` in the formatter config. Creates an empty
    manifest whose `:pubsub` is the `Phoenix.PubSub` that your application already starts.
    When the application starts no `Phoenix.PubSub`, the task adds one to its supervision
    tree.

    ## Options

      * `--manifest` - the module of the manifest to create. Defaults to `MyApp.Topics`.
      * `--pubsub` - the name of the `Phoenix.PubSub` that the manifest uses. Defaults to
        the one that the application starts, or else `MyApp.PubSub`.
    """

    use Igniter.Mix.Task

    @impl Igniter.Mix.Task
    def info(_argv, _composing_task) do
      %Igniter.Mix.Task.Info{
        group: :verified_pubsub,
        example: "mix igniter.install verified_pubsub",
        schema: [manifest: :string, pubsub: :string]
      }
    end

    @impl Igniter.Mix.Task
    def igniter(igniter) do
      options = igniter.args.options
      {igniter, started} = started_pubsub(igniter)

      requested = options[:pubsub] && Igniter.Project.Module.parse(options[:pubsub])
      default = Igniter.Project.Module.module_name(igniter, "PubSub")

      manifest =
        if options[:manifest] do
          Igniter.Project.Module.parse(options[:manifest])
        else
          Igniter.Project.Module.module_name(igniter, "Topics")
        end

      {igniter, pubsub} =
        case {requested, started} do
          {nil, {:ok, name}} ->
            {igniter, name}

          {nil, :none} ->
            {Igniter.Project.Application.add_new_child(igniter, pubsub_child(default)), default}

          {nil, :unreadable} ->
            {Igniter.add_warning(igniter, unreadable_warning(manifest)), default}

          {name, {:ok, name}} ->
            {igniter, name}

          {name, :none} ->
            {Igniter.Project.Application.add_new_child(igniter, pubsub_child(name)), name}

          # The application starts a different PubSub. Every `Phoenix.PubSub` child has the
          # same default id, so a second child fails to start. The task does not add one.
          {name, _} ->
            {Igniter.add_warning(igniter, not_started_warning(name)), name}
        end

      igniter
      |> Igniter.Project.Formatter.import_dep(:verified_pubsub)
      |> Igniter.Project.Module.find_and_update_or_create_module(
        manifest,
        "use VerifiedPubSub.Manifest, pubsub: #{inspect(pubsub)}",
        &{:ok, &1}
      )
    end

    # Finds a `{Phoenix.PubSub, name: ...}` child in the application module. Returns
    # `:unreadable` when the application starts one, but its name is not a literal module
    # or atom.
    defp started_pubsub(igniter) do
      with app when not is_nil(app) <- app_module(igniter),
           {:ok, {igniter, _source, zipper}} <- Igniter.Project.Module.find_module(igniter, app) do
        case Igniter.Code.Common.move_to(zipper, &pubsub_child?/1) do
          {:ok, child} -> {igniter, pubsub_name(child)}
          :error -> {igniter, :none}
        end
      else
        {:error, igniter} -> {igniter, :none}
        nil -> {igniter, :none}
      end
    end

    defp app_module(igniter) do
      case Igniter.Project.Application.app_module(igniter) do
        {app, _args} -> app
        app -> app
      end
    end

    defp pubsub_child?(zipper) do
      with true <- Igniter.Code.Tuple.tuple?(zipper),
           {:ok, module} <- Igniter.Code.Tuple.tuple_elem(zipper, 0) do
        module
        |> Igniter.Code.Common.expand_alias()
        |> Igniter.Code.Common.nodes_equal?(Phoenix.PubSub)
      else
        _ -> false
      end
    end

    defp pubsub_name(child) do
      with {:ok, opts} <- Igniter.Code.Tuple.tuple_elem(child, 1),
           {:ok, name} <- Igniter.Code.Keyword.get_key(opts, :name),
           {:ok, name} when is_atom(name) <- Igniter.Code.Common.expand_literal(name) do
        {:ok, name}
      else
        _ -> :unreadable
      end
    end

    defp pubsub_child(name), do: {Phoenix.PubSub, name: name}

    defp unreadable_warning(manifest) do
      """
      VerifiedPubSub: the application starts a Phoenix.PubSub, but the installer could not
      read the name it is started with. Set `pubsub:` in #{inspect(manifest)} to that name,
      or rerun the installer with `--pubsub`.
      """
    end

    defp not_started_warning(name) do
      """
      VerifiedPubSub: the application already starts a Phoenix.PubSub, so #{inspect(name)}
      was not added to its supervision tree. Start #{inspect(name)} yourself, giving it a
      distinct child id with `Supervisor.child_spec/2`.
      """
    end
  end
else
  defmodule Mix.Tasks.VerifiedPubsub.Install do
    @shortdoc "Installs VerifiedPubSub into a project"
    @moduledoc "#{@shortdoc}. Requires Igniter."

    use Mix.Task

    @impl Mix.Task
    def run(_argv) do
      Mix.shell().error("""
      The task 'verified_pubsub.install' requires igniter. Please install igniter and try
      again, or follow the installation steps in the VerifiedPubSub README.

      For more information, see: https://hexdocs.pm/igniter
      """)

      exit({:shutdown, 1})
    end
  end
end
