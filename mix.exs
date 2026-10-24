defmodule VerifiedPubSub.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/CuriousCurmudgeon/verified_pubsub"

  def project do
    [
      app: :verified_pubsub,
      version: @version,
      elixir: "~> 1.16",
      elixirc_paths: elixirc_paths(Mix.env()),
      # Spark records where DSL entities are declared only with debug_info on, and tests
      # assert on errors that point at a declaration.
      test_elixirc_options: [docs: false, debug_info: true],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "VerifiedPubSub",
      description: "Compile-time verified PubSub: verified routes, but for topics and events.",
      source_url: @source_url,
      homepage_url: @source_url,
      package: package(),
      docs: docs(),
      usage_rules: usage_rules()
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      files: ~w(lib .formatter.exs mix.exs README.md CHANGELOG.md LICENSE usage-rules.md),
      # Hex packages what is on disk, not what git tracks, so keep Vim swap files out.
      exclude_patterns: [~r/\.sw[a-p]$/],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "https://hexdocs.pm/verified_pubsub/changelog.html"
      }
    ]
  end

  defp docs do
    [
      main: "VerifiedPubSub",
      source_ref: "v#{@version}",
      extras: ["README.md", "usage-rules.md": [title: "Usage rules for LLMs"], "CHANGELOG.md": []],
      groups_for_modules: [
        "Declaring topics": [
          VerifiedPubSub.Manifest,
          VerifiedPubSub.Info,
          VerifiedPubSub.Topic
        ],
        "Broadcasting and subscribing": [
          VerifiedPubSub.Api,
          VerifiedPubSub.Subscriber,
          VerifiedPubSub.Message,
          VerifiedPubSub.PayloadError,
          VerifiedPubSub.TopicError,
          VerifiedPubSub.ManifestMismatchError
        ],
        Internals: [
          VerifiedPubSub.Broadcast,
          VerifiedPubSub.Dsl,
          VerifiedPubSub.Dsl.Topic,
          VerifiedPubSub.Dsl.Message,
          VerifiedPubSub.Dsl.Field,
          VerifiedPubSub.Payload,
          VerifiedPubSub.Subscriber.Verify,
          VerifiedPubSub.Transformers.ParseParams,
          VerifiedPubSub.Transformers.ValidateTopics,
          VerifiedPubSub.Transformers.ValidateFields,
          VerifiedPubSub.Transformers.DefinePayloadSchemas
        ]
      ]
    ]
  end

  # Run "mix usage_rules.sync" to write the rules of the dependencies into AGENTS.md.
  defp usage_rules do
    [
      file: "AGENTS.md",
      usage_rules: [{:usage_rules, except: [:otp]}]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:spark, "~> 2.7 and >= 2.7.4"},
      {:phoenix_pubsub, "~> 2.1"},
      {:igniter, "~> 0.6", optional: true},
      {:phoenix_live_view, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:usage_rules, "~> 1.2", only: :dev, runtime: false},
      {:benchee, "~> 1.3", only: :dev}
    ]
  end
end
