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

  @doc """
  Compiles `source` with docs on, and returns the moduledoc entry of the last module that
  it defines.

  The entry is `%{"en" => doc}`, `:none`, or `:hidden`. This function reads the docs from
  a `.beam` in `tmp_dir`, because `Code.fetch_docs/1` cannot read a module that exists only
  in memory. The test env turns docs off, so this function turns them on and then back.
  """
  def fetch_moduledoc!(tmp_dir, source) do
    previous = Code.get_compiler_option(:docs)
    Code.put_compiler_option(:docs, true)

    try do
      {module, binary} =
        source
        |> Code.compile_string()
        |> List.last()

      path = Path.join(tmp_dir, "#{inspect(module)}.beam")
      File.write!(path, binary)
      {:docs_v1, _anno, _language, _format, moduledoc, _metadata, _docs} = Code.fetch_docs(path)
      moduledoc
    after
      Code.put_compiler_option(:docs, previous)
    end
  end

  @doc "A unique module name, so that generated test modules never collide."
  def unique_module(prefix) do
    "#{prefix}#{System.unique_integer([:positive])}"
  end
end
