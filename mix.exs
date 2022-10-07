defmodule Wolff.MixProject do
  use Mix.Project

  def project() do
    [
      app: :wolff,
      version: read_version(),
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      erlc_paths: erlc_paths(Mix.env()),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {:wolff_app, []}
    ]
  end

  def elixirc_paths(:test), do: ["test", "src", "lib"]
  def elixirc_paths(_), do: ["src", "lib"]

  def erlc_paths(:test), do: ["test", "src"]
  def erlc_paths(_), do: ["src"]

  defp deps do
    [
      {:kafka_protocol, "4.1.0"},
      {:replayq, "0.3.4"},
      {:lc, "0.3.2"},
      {:telemetry, "1.1.0"},
      {:meck, "~> 0.9", only: [:test, :dev]},
      {:redbug, "~> 2.0", only: [:test, :dev]},
    ]
  end

  defp read_version() do
    try do
      {out, 0} = System.cmd("git", ["describe", "--tags"])
      out
      |> String.trim()
      |> Version.parse!()
      |> Map.put(:pre, [])
      |> to_string()
    rescue
      _ -> "0.1.0"
    end
  end
end
