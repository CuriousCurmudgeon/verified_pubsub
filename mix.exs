defmodule VerifiedPubSub.MixProject do
  use Mix.Project

  def project do
    [
      app: :verified_pubsub,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "VerifiedPubSub",
      description: "Compile-time verified PubSub: verified routes, but for topics and events.",
      package: package(),
      docs: docs()
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE),
      links: %{}
    ]
  end

  defp docs do
    [
      main: "VerifiedPubSub",
      extras: ["README.md"],
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
      {:spark, "~> 2.7"},
      {:phoenix_pubsub, "~> 2.1"},
      {:phoenix_live_view, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false}
    ]
  end
end
