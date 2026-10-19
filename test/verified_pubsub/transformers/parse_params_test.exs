defmodule VerifiedPubSub.Transformers.ParseParamsTest do
  use ExUnit.Case, async: true

  alias VerifiedPubSub.Info
  alias VerifiedPubSub.TestManifests.Basic
  alias VerifiedPubSub.Transformers.ParseParams

  describe "parse/1" do
    test "extracts params in order" do
      assert {:ok, [:owner_id]} = ParseParams.parse(["owners", :owner_id, "pets"])
      assert {:ok, [:org_id, :user_id]} = ParseParams.parse(["o", :org_id, "u", :user_id])
    end

    test "returns an empty list for a static topic" do
      assert {:ok, []} = ParseParams.parse(["system"])
    end

    test "rejects an empty segment list" do
      assert {:error, msg} = ParseParams.parse([])
      assert msg =~ "at least one segment"
    end

    test "rejects an empty literal" do
      assert {:error, msg} = ParseParams.parse(["owners", ""])
      assert msg =~ "empty"
    end

    # A literal carrying the separator would be several segments pretending to be one,
    # which hides them from the disjointness check.
    test "rejects a literal containing the separator" do
      assert {:error, msg} = ParseParams.parse(["owners:pets"])
      assert msg =~ "owners:pets"
      assert msg =~ "its own list element"
    end

    test "rejects nil and booleans as params" do
      for atom <- [nil, true, false] do
        assert {:error, msg} = ParseParams.parse(["a", atom])
        assert msg =~ "not a parameter name"
      end
    end

    test "rejects a duplicate parameter name" do
      assert {:error, msg} = ParseParams.parse(["a", :id, "b", :id])
      assert msg =~ "duplicate"
      assert msg =~ "id"
    end
  end

  describe "Info.params/2" do
    test "params land on the topic struct" do
      assert [:owner_id] = Info.params(Basic, :pets)
      assert [] = Info.params(Basic, :system)
    end
  end
end
