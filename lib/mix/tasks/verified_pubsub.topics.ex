defmodule Mix.Tasks.VerifiedPubsub.Topics do
  @shortdoc "Prints where each topic and event is in use"

  @moduledoc """
  #{@shortdoc}.

      mix verified_pubsub.topics [TOPIC [EVENT]] [--summary] [--manifest MODULE] [--format text|json]

  The task prints each call site of each topic, as `file:line`, grouped by event. A call
  site is a `subscribe`, `unsubscribe`, `broadcast`, `broadcast_from`, `handle_message`, or
  `ignore_message` call. An event that no call broadcasts gets the flag `(no broadcast)`.

      mix verified_pubsub.topics

  A topic limits the output to that topic. An event limits it to that event, and the
  subscribes of its topic.

      mix verified_pubsub.topics pets
      mix verified_pubsub.topics pets adopted

  `--summary` prints a table for each manifest in the place of the call sites. The table
  gives each topic, its wire pattern, and the number of its events, broadcasts, handlers,
  and subscribes. A topic and an event limit the table in the same way.

      mix verified_pubsub.topics --summary

  The task compiles the project first. It reads the call sites that the macros record in
  each compiled module, so it does not start the application. It runs in the current
  `MIX_ENV`. Thus a call site in `test/support` shows only with `MIX_ENV=test`. An `.exs`
  file, such as a test, records nothing, because the task does not compile it.

  ## Options

    * `--summary` - print the counts of each topic, not the call sites.
    * `--manifest` - the manifest to report, as `MyApp.Topics`. Defaults to each manifest.
    * `--format` - `text` or `json`. Defaults to `text`. `json` needs Elixir 1.18 or later.
  """

  use Mix.Task

  alias VerifiedPubSub.Usage.{Json, Report, Text}

  @usage "mix verified_pubsub.topics [TOPIC [EVENT]] [--summary] [--manifest MODULE] " <>
           "[--format text|json]"

  @impl Mix.Task
  def run(args) do
    {opts, positional} =
      OptionParser.parse!(args, strict: [manifest: :string, format: :string, summary: :boolean])

    filters = [manifest: opts[:manifest], summary: opts[:summary]] ++ filters(positional)

    format = Keyword.get(opts, :format, "text")
    quietly(format, fn -> Mix.Task.run("compile") end)

    case Report.build(project_modules(), filters) do
      {:ok, report} -> print(report, format)
      {:error, message} -> Mix.raise(message)
    end
  end

  defp filters([]), do: []
  defp filters([topic]), do: [topic: topic]
  defp filters([topic, event]), do: [topic: topic, event: event]
  defp filters(_positional), do: Mix.raise("expected at most two arguments.\n\nUsage: #{@usage}")

  # An umbrella compiles each child app, and a manifest in one app can serve the modules of
  # another. So the report reads the modules of every app together.
  defp project_modules do
    apps =
      if Mix.Project.umbrella?() do
        Map.keys(Mix.Project.apps_paths())
      else
        [Mix.Project.config()[:app]]
      end

    Enum.flat_map(apps, fn app ->
      _ = Application.load(app)
      Application.spec(app, :modules) || []
    end)
  end

  defp print(report, "text"), do: Mix.shell().info(Text.render(report))

  defp print(report, "json") do
    case Json.render(report) do
      {:ok, json} -> Mix.shell().info(json)
      {:error, message} -> Mix.raise(message)
    end
  end

  defp print(_report, format) do
    Mix.raise("unknown format #{inspect(format)}. Use text or json.")
  end

  @doc false
  # The compile prints its progress, as `Compiling 2 files (.ex)`, to stdout through the
  # Mix shell. That text would come before the JSON, and a parser would reject the output.
  # Compiler warnings and errors go to stderr, so they still show.
  @spec quietly(String.t(), (-> result)) :: result when result: term()
  def quietly("json", fun) do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Quiet)

    try do
      fun.()
    after
      Mix.shell(shell)
    end
  end

  def quietly(_format, fun), do: fun.()
end
