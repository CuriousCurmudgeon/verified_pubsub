defmodule VerifiedPubsub.CompileHelper do
  @moduledoc "Helpers for asserting on compile-time success and failure."

  @doc "Compiles `source`, returning the first module defined. Raises on failure."
  def compile!(source) do
    [{module, _bin} | _] = Code.compile_string(source)
    module
  end

  @doc """
  Compiles `source` and returns the exception it raised, or `nil` if it compiled.
  """
  def compile_error(source) do
    Code.compile_string(source)
    nil
  rescue
    exception -> exception
  end

  @doc "A unique module name, so generated test modules never collide."
  def unique_module(prefix) do
    "#{prefix}#{System.unique_integer([:positive])}"
  end
end
