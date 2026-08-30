defmodule VerifiedPubsub.MixProject do
  use Mix.Project

  def project do
    [
      app: :verified_pubsub,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "VerifiedPubsub",
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
      main: "VerifiedPubsub",
      extras: ["README.md"],
      groups_for_modules: [
        Registry: [
          VerifiedPubsub.Registry,
          VerifiedPubsub.Dsl,
          VerifiedPubsub.Info
        ],
        Subscribing: [
          VerifiedPubsub.Subscriber,
          VerifiedPubsub.Message
        ],
        Internals: [
          VerifiedPubsub.Broadcast
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
      {:phoenix_live_view, "~> 1.0", only: :test}
    ]
  end
end
