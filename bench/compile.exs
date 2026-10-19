# Compile-time cost of VerifiedPubSub over plain Phoenix.PubSub.
#
#     elixir bench/compile.exs [out_dir]
#
# This script generates Mix projects that do the same work in two ways. One way uses a
# VerifiedPubSub manifest, broadcast!/4, and handle_message/5. The other way uses wire
# topics built by hand, Phoenix.PubSub.broadcast!/3, and handle_info/2. The script times
# a clean compile of each project. For each verified project, it also times an
# incremental compile after an edit to the manifest.
#
# The projects share the deps directory, the lockfile, and the .tool-versions of this
# repository. So the script fetches nothing, and every project uses the same Elixir. Each
# project compiles its dependencies once, outside the timed compiles.
#
# Spark stores the source file path of each DSL entity in the manifest. So the compile
# time of the manifest grows with the length of out_dir. Use a path about as long as the
# path of a real project.

defmodule Bench.Compile do
  @repo Path.expand("..", __DIR__)
  @runs 3

  # {name, topics, events per topic, fields per event, caller modules, broadcasts per
  # caller, subscriber modules}. Subscriber i handles every event of topic i.
  @scenarios [
    {"small", 20, 5, 5, 50, 10, 20},
    {"medium", 100, 5, 5, 200, 10, 100},
    {"large", 250, 5, 5, 500, 10, 250}
  ]

  def run(out_dir) do
    File.mkdir_p!(out_dir)

    results =
      for {name, t, e, f, c, b, s} <- @scenarios do
        IO.puts(
          "\n== #{name}: #{t} topics x #{e} events x #{f} fields " <>
            "(#{t * e} events, #{t * e * f} fields), #{c} callers x #{b} broadcasts, " <>
            "#{s} subscribers"
        )

        shape = %{topics: t, events: e, fields: f, callers: c, broadcasts: b, subscribers: s}

        plain = project(out_dir, "#{name}_plain", :plain, shape)
        verified = project(out_dir, "#{name}_verified", :verified, shape)

        plain_ms = clean_compile(plain)
        verified_ms = clean_compile(verified)
        manifest_ms = manifest_only(out_dir, name, shape)

        IO.puts("  clean compile, plain:         #{fmt(plain_ms)}")
        IO.puts("  clean compile, verified:      #{fmt(verified_ms)}")
        IO.puts("  manifest module alone:        #{fmt(manifest_ms)}")

        beam = Path.join(verified, "_build/dev/lib/bench_app/ebin/Elixir.BenchApp.Topics.beam")
        IO.puts("  manifest .beam size:          #{div(File.stat!(beam).size, 1024)} KB")

        incremental =
          for {label, edit} <- [
                {"edit a description", &edit_description/1},
                {"add an optional field", &add_optional_field/1}
              ] do
            {ms, recompiled} = incremental_compile(verified, edit)

            IO.puts(
              "  after #{String.pad_trailing(label <> ":", 23)}#{fmt(ms)}, " <>
                "#{recompiled} files recompiled"
            )

            {label, ms, recompiled}
          end

        {name, plain_ms, verified_ms, manifest_ms, incremental}
      end

    # "everything else" is the verified compile minus the compile of the manifest. It
    # covers the callers and the subscribers, for comparison with the plain compile.
    IO.puts("\nscenario   plain      verified   manifest   everything else")

    for {name, plain_ms, verified_ms, manifest_ms, _} <- results do
      IO.puts(
        String.pad_trailing(name, 11) <>
          String.pad_trailing(fmt(plain_ms), 11) <>
          String.pad_trailing(fmt(verified_ms), 11) <>
          String.pad_trailing(fmt(manifest_ms), 11) <>
          fmt(max(verified_ms - manifest_ms, 0))
      )
    end
  end

  # -- compile time --------------------------------------------------------------------

  # Returns the median of a few forced compiles, in ms. The times come from
  # `--profile time`, so they leave out the startup of Mix.
  defp clean_compile(dir) do
    1..@runs
    |> Enum.map(fn _ -> compile_ms(dir, ["--force"]) end)
    |> median()
  end

  defp incremental_compile(dir, edit) do
    manifest = Path.join(dir, "lib/topics.ex")
    original = File.read!(manifest)

    try do
      # Mix compares sources by content digest and mtime. The sleep lets Mix see the edit.
      Process.sleep(1_100)
      File.write!(manifest, edit.(original))
      {output, ms} = mix_compile(dir, ["--verbose"])
      {ms, recompiled(output)}
    after
      Process.sleep(1_100)
      File.write!(manifest, original)
      compile_ms(dir, [])
    end
  end

  defp compile_ms(dir, args) do
    {_output, ms} = mix_compile(dir, args)
    ms
  end

  defp mix_compile(dir, args) do
    {output, status} =
      System.cmd("mix", ["compile", "--profile", "time" | args],
        cd: dir,
        stderr_to_stdout: true,
        env: [{"MIX_ENV", "dev"}]
      )

    if status != 0, do: raise("mix compile failed in #{dir}:\n#{output}")

    # Add up every phase that the profiler reports:
    #
    #   * the compile cycle
    #   * the write to disk
    #   * the after-compile callbacks
    #   * the group pass that type-checks the modules
    ms =
      ~r/\[profile\] Finished .* in (\d+)ms/
      |> Regex.scan(output)
      |> Enum.map(fn [_, ms] -> String.to_integer(ms) end)
      |> Enum.sum()

    {output, ms}
  end

  # With --verbose, Mix names each file that it compiles.
  defp recompiled(output) do
    ~r/Compiled lib\/\S+\.ex/ |> Regex.scan(output) |> length()
  end

  # A project that holds only the manifest. It separates the compile time of the manifest
  # from the cost that the manifest adds to each caller.
  defp manifest_only(out_dir, name, shape) do
    dir =
      project(out_dir, "#{name}_manifest_only", :verified, %{shape | callers: 0, subscribers: 0})

    clean_compile(dir)
  end

  defp median(list), do: list |> Enum.sort() |> Enum.at(div(length(list), 2))

  defp fmt(ms) when ms >= 1_000, do: "#{Float.round(ms / 1_000, 2)}s"
  defp fmt(ms), do: "#{ms}ms"

  # -- manifest edits -----------------------------------------------------------------

  defp edit_description(source) do
    String.replace(source, ~s(description "Topic 1."), ~s(description "Topic one."),
      global: false
    )
  end

  defp add_optional_field(source) do
    String.replace(
      source,
      "      field :f1, :string\n",
      "      field :f1, :string\n      field :extra, :string, required: false\n",
      global: false
    )
  end

  # -- project generation -------------------------------------------------------------

  defp project(out_dir, name, kind, shape) do
    dir = Path.join(out_dir, name)
    File.rm_rf!(Path.join(dir, "lib"))
    File.mkdir_p!(Path.join(dir, "lib/callers"))
    File.mkdir_p!(Path.join(dir, "lib/subscribers"))

    File.write!(Path.join(dir, "mix.exs"), mix_exs(kind))
    File.cp!(Path.join(@repo, "mix.lock"), Path.join(dir, "mix.lock"))
    File.cp!(Path.join(@repo, ".tool-versions"), Path.join(dir, ".tool-versions"))

    if kind == :verified do
      File.write!(Path.join(dir, "lib/topics.ex"), manifest(shape))
    end

    for i <- 1..shape.callers//1 do
      File.write!(Path.join(dir, "lib/callers/caller#{i}.ex"), caller(kind, i, shape))
    end

    for i <- 1..shape.subscribers//1 do
      File.write!(Path.join(dir, "lib/subscribers/sub#{i}.ex"), subscriber(kind, i, shape))
    end

    # Compile deps once, with no timer. A forced compile of only the app follows.
    {output, status} =
      System.cmd("mix", ["do", "deps.get,", "compile"],
        cd: dir,
        stderr_to_stdout: true,
        env: [{"MIX_ENV", "dev"}]
      )

    if status != 0, do: raise("setup failed in #{dir}:\n#{output}")
    dir
  end

  defp mix_exs(kind) do
    dep =
      case kind do
        :verified -> ~s({:verified_pubsub, path: #{inspect(@repo)}})
        :plain -> ~s({:phoenix_pubsub, "~> 2.1"})
      end

    """
    defmodule BenchApp.MixProject do
      use Mix.Project

      def project do
        [
          app: :bench_app,
          version: "0.1.0",
          deps_path: #{inspect(Path.join(@repo, "deps"))},
          deps: [#{dep}]
        ]
      end
    end
    """
  end

  defp manifest(shape) do
    topics =
      for t <- 1..shape.topics do
        messages =
          for e <- 1..shape.events do
            fields = for f <- 1..shape.fields, do: "      field :f#{f}, :string\n"

            "    message :e#{e} do\n#{fields}    end\n"
          end

        """
          topic :t#{t}, ["t#{t}", :id] do
            description "Topic #{t}."

        #{Enum.join(messages, "\n")}  end
        """
      end

    """
    defmodule BenchApp.Topics do
      use VerifiedPubSub.Manifest, pubsub: BenchApp.PubSub

    #{Enum.join(topics, "\n")}end
    """
  end

  # Broadcast j of caller i goes to a topic and an event from across the manifest.
  defp target(i, j, shape) do
    n = (i - 1) * shape.broadcasts + (j - 1)
    {rem(n, shape.topics) + 1, rem(div(n, shape.topics), shape.events) + 1}
  end

  defp caller(kind, i, shape) do
    args = Enum.map_join(1..shape.fields, ", ", &"a#{&1}")
    payload = Enum.map_join(1..shape.fields, ", ", &"f#{&1}: a#{&1}")

    use_line =
      if kind == :verified, do: "  use VerifiedPubSub, manifest: BenchApp.Topics\n\n", else: ""

    functions =
      for j <- 1..shape.broadcasts do
        {t, e} = target(i, j, shape)

        call =
          case kind do
            :verified ->
              "broadcast!(:t#{t}, %{id: id}, :e#{e}, %{#{payload}})"

            :plain ->
              ~s[Phoenix.PubSub.broadcast!(BenchApp.PubSub, "t#{t}:" <> id, {:e#{e}, %{#{payload}}})]
          end

        "  def b#{j}(id, #{args}) do\n    #{call}\n  end\n"
      end

    "defmodule BenchApp.Caller#{i} do\n#{use_line}#{Enum.join(functions, "\n")}end\n"
  end

  defp subscriber(kind, i, shape) do
    topic = rem(i - 1, shape.topics) + 1
    pattern = Enum.map_join(1..shape.fields, ", ", &"f#{&1}: _")

    {use_line, clauses} =
      case kind do
        :verified ->
          {"  use VerifiedPubSub.Subscriber, manifest: BenchApp.Topics\n\n",
           for e <- 1..shape.events do
             "  handle_message :t#{topic}, :e#{e}, %{#{pattern}}, state do\n" <>
               "    {:noreply, state}\n  end\n"
           end}

        :plain ->
          {"",
           for e <- 1..shape.events do
             "  def handle_info({:e#{e}, %{#{pattern}}}, state) do\n" <>
               "    {:noreply, state}\n  end\n"
           end}
      end

    "defmodule BenchApp.Sub#{i} do\n#{use_line}#{Enum.join(clauses, "\n")}end\n"
  end
end

out_dir =
  case System.argv() do
    [dir] -> Path.expand(dir)
    [] -> Path.join(System.tmp_dir!(), "verified_pubsub_compile_bench")
  end

Bench.Compile.run(out_dir)
