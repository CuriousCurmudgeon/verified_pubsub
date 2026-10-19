defmodule VerifiedPubSub.DocsTest do
  # Not async, because the `:docs` compiler option is global and the test env turns it off.
  use ExUnit.Case, async: false

  import VerifiedPubSub.CompileHelper

  @moduletag :tmp_dir

  test "appends the topics to a hand-written moduledoc", %{tmp_dir: tmp_dir} do
    doc =
      moduledoc!(tmp_dir, """
      @moduledoc "Hand written."

      use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

      topic :system, ["system"] do
        message :alert do
          field :text, :string
        end
      end
      """)

    assert doc == """
           Hand written.

           ## Topics

           ### `:system`

           Wire topic: `"system"`

           Events:

           * `:alert`
             * `:text` (`:string`)
           """
  end

  test "documents a manifest that has no moduledoc", %{tmp_dir: tmp_dir} do
    doc =
      moduledoc!(tmp_dir, """
      use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

      topic :system, ["system"] do
        message :alert
      end
      """)

    assert doc == """
           ## Topics

           ### `:system`

           Wire topic: `"system"`

           Events:

           * `:alert`
           """
  end

  test "finds a moduledoc below the use", %{tmp_dir: tmp_dir} do
    doc =
      moduledoc!(tmp_dir, """
      use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

      @moduledoc "Written last."

      topic :system, ["system"] do
        message :alert
      end
      """)

    assert doc =~ ~r/\AWritten last\.\n\n## Topics\n/
  end

  test "keeps a manifest with @moduledoc false hidden", %{tmp_dir: tmp_dir} do
    assert :hidden ==
             docs!(tmp_dir, """
             @moduledoc false

             use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

             topic :system, ["system"] do
               message :alert
             end
             """)
  end

  test "adds no section to a manifest that declares no topics", %{tmp_dir: tmp_dir} do
    doc =
      moduledoc!(tmp_dir, """
      @moduledoc "No topics yet."

      use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub
      """)

    assert doc == "No topics yet.\n"
  end

  test "renders params, descriptions, types and optional fields", %{tmp_dir: tmp_dir} do
    doc =
      moduledoc!(tmp_dir, ~S'''
      use VerifiedPubSub.Manifest, pubsub: VerifiedPubSub.TestPubSub

      topic :pets, ["owners", :owner_id, "pets"] do
        description "Pets of one owner."

        message :adopted do
          description "An owner adopted a pet."

          field :pet, VerifiedPubSub.TestStructs.Point, description: "The adopted pet."
          field :tricks, {:list, :string}
          field :nickname, :string, required: false, description: "Set by the owner."
        end

        message :rehomed do
          description """
          The pet moved to a new owner.

          Sent to the previous owner.
          """

          field :id, :string
        end

        message :forgotten, description: "The owner forgot the pet."
      end
      ''')

    assert doc == """
           ## Topics

           ### `:pets`

           Pets of one owner.

           Wire topic: `"owners:<owner_id>:pets"`

           Events:

           * `:adopted` - An owner adopted a pet.
             * `:pet` (`VerifiedPubSub.TestStructs.Point`) - The adopted pet.
             * `:tricks` (`{:list, :string}`)
             * `:nickname` (`:string`, optional) - Set by the owner.
           * `:rehomed` - The pet moved to a new owner.

             Sent to the previous owner.
             * `:id` (`:string`)
           * `:forgotten` - The owner forgot the pet.
           """
  end

  # Spark ends the moduledoc with a different number of newlines when the manifest has a
  # hand-written moduledoc, so the comparison ignores trailing newlines.
  defp moduledoc!(tmp_dir, body) do
    %{"en" => doc} = docs!(tmp_dir, body)
    String.trim_trailing(doc) <> "\n"
  end

  # Compiles a manifest with docs on, and returns its moduledoc entry. This function reads
  # the docs from the .beam, because Code.fetch_docs/1 cannot read a module that exists
  # only in memory.
  defp docs!(tmp_dir, body) do
    name = unique_module("VerifiedPubSub.DocsTest.Manifest")
    previous = Code.get_compiler_option(:docs)
    Code.put_compiler_option(:docs, true)

    try do
      [{module, binary} | _] = Code.compile_string("defmodule #{name} do\n#{body}\nend")
      path = Path.join(tmp_dir, "#{inspect(module)}.beam")
      File.write!(path, binary)
      {:docs_v1, _anno, _language, _format, moduledoc, _metadata, _docs} = Code.fetch_docs(path)
      moduledoc
    after
      Code.put_compiler_option(:docs, previous)
    end
  end
end
