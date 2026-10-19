defmodule VerifiedPubSub.CompileHelper do
  @moduledoc "Helpers for tests that check whether source compiles."

  @doc "Compiles `source`, and returns the first module that it defines. Raises on failure."
  def compile!(source) do
    [{module, _bin} | _] = Code.compile_string(source)
    module
  end

  @doc """
  Compiles `source`, and returns the exception that it raised, or `nil` when it compiled.
  """
  def compile_error(source) do
    Code.compile_string(source)
    nil
  rescue
    exception -> exception
  end

  @doc """
  Like `compile_error/1`, for a manifest that the DSL rejects.

  Spark reports a DSL error twice: as an exception, and as a diagnostic at the declaration
  that caused it. This function drops the diagnostic, so that it stays out of the test
  output.
  """
  def dsl_error(source) do
    {error, _diagnostics} = Code.with_diagnostics(fn -> compile_error(source) end)
    error
  end

  @doc """
  A unique owner id.

  The suite shares one `VerifiedPubSub.TestManifests.Basic` manifest. A concurrent test that
  broadcasts on a wire topic delivers to each test that subscribes to it. So a test that
  subscribes must use a unique param value.
  """
  def unique_owner_id, do: "owner#{System.unique_integer([:positive])}"

  @doc "A unique module name, so that generated test modules never collide."
  def unique_module(prefix) do
    "#{prefix}#{System.unique_integer([:positive])}"
  end
end
