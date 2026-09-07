defmodule VerifiedPubSub.CompileHelper do
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

  @doc """
  A unique account id.

  The suite shares one `VerifiedPubSub.Adapter.Local` manifest, so any test that
  subscribes must use a unique param value or a concurrent test broadcasting on the
  same topic string will deliver to it.
  """
  def unique_account_id, do: "acct#{System.unique_integer([:positive])}"

  @doc "A unique module name, so generated test modules never collide."
  def unique_module(prefix) do
    "#{prefix}#{System.unique_integer([:positive])}"
  end
end
