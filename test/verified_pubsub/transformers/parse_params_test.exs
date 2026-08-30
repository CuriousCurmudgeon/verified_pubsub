defmodule VerifiedPubsub.Transformers.ParseParamsTest do
  use ExUnit.Case, async: true

  alias VerifiedPubsub.Info
  alias VerifiedPubsub.TestRegistries.Basic
  alias VerifiedPubsub.Transformers.ParseParams

  describe "parse/1" do
    test "extracts params in order" do
      assert {:ok, [:account_id]} = ParseParams.parse("accounts:%{account_id}:campaigns")
      assert {:ok, [:org_id, :user_id]} = ParseParams.parse("o:%{org_id}:u:%{user_id}")
    end

    test "returns an empty list for a static pattern" do
      assert {:ok, []} = ParseParams.parse("system")
    end

    test "rejects an unclosed parameter" do
      assert {:error, msg} = ParseParams.parse("accounts:%{account_id")
      assert msg =~ "unterminated"
    end

    test "rejects an empty parameter name" do
      assert {:error, msg} = ParseParams.parse("accounts:%{}")
      assert msg =~ "empty"
    end

    test "rejects a duplicate parameter name" do
      assert {:error, msg} = ParseParams.parse("a:%{id}:b:%{id}")
      assert msg =~ "duplicate"
      assert msg =~ "id"
    end

    test "rejects a parameter name that is not a valid identifier" do
      assert {:error, msg} = ParseParams.parse("a:%{account-id}")
      assert msg =~ "account-id"
    end
  end

  describe "integration with the DSL" do
    test "params land on the topic struct" do
      assert [:account_id] = Info.params(Basic, :campaigns)
      assert [] = Info.params(Basic, :system)
    end
  end
end
