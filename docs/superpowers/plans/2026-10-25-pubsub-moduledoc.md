# PubSub Section in the Moduledoc Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A module that turns on the `docs` option gets a generated `## PubSub` section at the end of its
`@moduledoc`. The section lists the topics and events that the module publishes and handles.

**Architecture:** The `broadcast*` and `subscribe` macros record `{topic, event}` and topics in accumulating module
attributes while they expand. Each `@before_compile` hook of the library returns code that ends with a call to
`VerifiedPubSub.Docs.put_moduledoc/1`. In the Subscriber, that call comes after the dispatch clauses, so a `broadcast!`
in a `handle_message` body has expanded before the call runs. `put_moduledoc/1` is idempotent: it keeps the moduledoc
that the user wrote and rewrites the section each time, so the last call wins with complete data.

**Tech Stack:** Elixir 1.16+, Spark (`opt_schema` of the manifest), ExUnit.

**Spec:** `docs/superpowers/specs/2026-10-25-pubsub-moduledoc-design.md`

## Global Constraints

- Branch: `pubsub-moduledoc`, from `main`. The change ships in v0.3.0. Do not change `@version` in `mix.exs`.
- The header is exactly `## PubSub`.
- Bullet text is exactly ``  * Publishes to `"<wire pattern>"` for <events>.`` and
  ``  * Subscribes to `"<wire pattern>"` for <events>.``, or ``  * Subscribes to `"<wire pattern>"`.`` with no events.
  Each bullet starts with two spaces and is one line.
- The wire pattern is `inspect(VerifiedPubSub.Topic.pattern(topic.segments))`. Each event is `` `#{inspect(event)}` ``.
- Event list: one event `:a`; two events `:a` and `:b`; three or more `:a`, `:b`, and `:c`.
- Publishes bullets come first, then Subscribes bullets. Each group sorts by wire pattern. Events sort by name.
- `docs` defaults to `false`. No application configuration.
- Follow `AGENTS/STYLE.md` and `AGENTS/DOCS.md`: STE prose in docs, comments, test names, and commit messages. Test
  names follow `SPEC-1` to `SPEC-3`. Let logs print (`TEST-1`).
- After each task, run in order: `mix format`, `mix compile --warnings-as-errors`, `mix test`. Fix each issue.
- End each commit message with `Assisted-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`. Do not push.

## Review Focus

1. A `@moduledoc` that the user writes below the broadcasts, at the end of the module. The section still comes after
   it. Test in Task 1.
2. A module whose hook runs twice: both `use` lines in either order, or `use VerifiedPubSub` twice. It gets exactly one
   section. Tests in Task 2.
3. A heredoc `@moduledoc` that ends with a newline. Exactly one blank line comes before `## PubSub`. Test in Task 1.
4. An event or topic name that `inspect/1` quotes, as `:"bulk-update"`. The bullet shows `` `:"bulk-update"` ``. Test
   in Task 1 with an inline manifest.
5. A module with docs on, compiled while the `:docs` compiler option is off, as in a release build. It compiles, and its
   broadcasts work. Test in Task 1.

---

### Task 1: Publishes section for `use VerifiedPubSub`

**Files:**
- Modify: `lib/verified_pubsub.ex` (`__using__/1`, `bind_manifest!/2`, new `bind_docs!/2`, new `__before_compile__/1`,
  moduledoc)
- Modify: `lib/verified_pubsub/api.ex` (`build/7` records each broadcast)
- Modify: `lib/verified_pubsub/docs.ex` (new `put_moduledoc/1` and its private render functions)
- Modify: `test/support/compile_helper.ex` (new `fetch_moduledoc!/2`)
- Modify: `test/verified_pubsub/docs_test.exs` (use `fetch_moduledoc!/2`)
- Create: `test/verified_pubsub/pubsub_docs_test.exs`

**Interfaces:**
- Produces:
  - `VerifiedPubSub.bind_docs!(module(), boolean() | nil) :: :ok`, `@doc false`. `nil` records nothing. A boolean
    records `@verified_pubsub_docs`. A second, different boolean raises `ArgumentError` with the message
    `"#{inspect(module)} sets both docs: true and docs: false. Give one value."`. A value that is not a boolean or
    `nil` raises `ArgumentError` with `"expected a boolean for :docs, got: #{inspect(value)}"`.
  - `bind_manifest!/2` also registers `:verified_pubsub_broadcasts` and `:verified_pubsub_subscriptions` with
    `accumulate: true`, in its `nil` branch.
  - Module attribute `@verified_pubsub_broadcasts`: entries `{topic_name :: atom(), event :: atom()}`.
  - `VerifiedPubSub.Docs.put_moduledoc(module()) :: :ok`, `@doc false`. Idempotent.
  - `VerifiedPubSub.CompileHelper.fetch_moduledoc!(tmp_dir :: Path.t(), source :: String.t())` returns the moduledoc
    entry of `Code.fetch_docs/1`: `%{"en" => String.t()} | :none | :hidden`. `source` is the full source of one module.

- [ ] **Step 1: Move the docs helper.** Move the `.beam` logic of `docs!/2` in `docs_test.exs` into
  `CompileHelper.fetch_moduledoc!/2`. Keep the compiler option save and restore. `docs_test.exs` builds its source
  with `"defmodule #{name} do\n#{body}\nend"` and calls the new helper. Run `mix test test/verified_pubsub/docs_test.exs`.
  Expected: PASS.

- [ ] **Step 2: Write the failing tests** in `pubsub_docs_test.exs`. Module attributes: `async: false` and
  `@moduletag :tmp_dir`, as in `docs_test.exs`. Use `VerifiedPubSub.TestManifests.Basic` with `docs: true` on the
  `use` line. A private `moduledoc!/2` trims trailing whitespace and adds one `"\n"`, as in `docs_test.exs`.

  `describe "use VerifiedPubSub with docs on"`:

  - `"lists each published topic once, with its events"`. Body:

    ```elixir
    @moduledoc "Manages pets."
    use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

    def adopt(id), do: broadcast!(:pets, %{owner_id: id}, :adopted, %{id: "p1"})
    def adopt_again(id), do: broadcast(:pets, %{owner_id: id}, :adopted, %{id: "p2"})
    def rehome(id), do: notify(id)
    def alert, do: broadcast!(:system, :alert, %{text: "hi"})
    defp notify(id), do: broadcast_from(self(), :pets, %{owner_id: id}, :rehomed, %{id: "p1"})
    ```

    Expected moduledoc:

    ```text
    Manages pets.

    ## PubSub

      * Publishes to `"owners:<owner_id>:pets"` for `:adopted` and `:rehomed`.
      * Publishes to `"system"` for `:alert`.
    ```

  - `"adds the section after a moduledoc below the broadcasts"`: one `broadcast!(:system, :alert, ...)` in a `def`,
    then a heredoc `@moduledoc` with the text `Alerts.` at the end of the module. Expected
    ``"Alerts.\n\n## PubSub\n\n  * Publishes to `\"system\"` for `:alert`.\n"``.
  - `"writes the section as the whole moduledoc when the module has none"`: expected
    ``"## PubSub\n\n  * Publishes to `\"system\"` for `:alert`.\n"``.
  - `"keeps a module with @moduledoc false hidden"`: `fetch_moduledoc!/2` returns `:hidden`.
  - `"adds no section to a module with no broadcast"`: no `@moduledoc`, no macro calls. Returns `:none`.
  - `"quotes an event name that needs quotes"`: the source holds two modules. First an inline manifest with
    `pubsub: VerifiedPubSub.TestPubSub` and `topic :bulk, ["bulk"]` with `message :"bulk-update"`. Then the module
    under test, which broadcasts it. Make `fetch_moduledoc!/2` read the beam of the *last* module in the compile result
    so that this works, and so that the other tests still pass. Expected bullet
    ``  * Publishes to `"bulk"` for `:"bulk-update"`.``.

  `describe "use VerifiedPubSub with docs off"`:

  - `"adds no section by default"`: the `use` line has no `docs`. A `@moduledoc "Pets."` stays `"Pets.\n"`.

  `describe "use VerifiedPubSub with docs on and the :docs compiler option off"`:

  - `"compiles, and its broadcasts still deliver"`: compile with `CompileHelper.compile!/1` (docs stay off in the test
    env). Subscribe the test process to `:system` with `Phoenix.PubSub.subscribe(VerifiedPubSub.TestPubSub, "system")`,
    call the broadcasting function, and `assert_receive %VerifiedPubSub.Message{event: :alert}`.

- [ ] **Step 3: Run the tests.** Run: `mix test test/verified_pubsub/pubsub_docs_test.exs`. Expected: FAIL. The
  `docs` option is unknown and no section exists.

- [ ] **Step 4: Implement.**
  - `VerifiedPubSub.__using__/1`: call `bind_docs!(module, Keyword.get(opts, :docs))` after `bind_manifest!/2`. Add
    `@before_compile VerifiedPubSub` to the quote.
  - `VerifiedPubSub.__before_compile__/1`: return `quote do: VerifiedPubSub.Docs.put_moduledoc(__MODULE__)`. It takes
    `@doc false`.
  - `Api.build/7`: after the validations, `Module.put_attribute(caller.module, :verified_pubsub_broadcasts,
    {topic_struct.name, event})`.
  - `Docs.put_moduledoc/1`: when docs are off for the module, return `:ok`. Docs are on when
    `@verified_pubsub_docs` is `true`. (Task 3 adds the manifest default.) Otherwise read the moduledoc. On the first
    call, save it in `@verified_pubsub_user_moduledoc` (`{line, doc} | nil | {line, false}`). Each later call reads the
    saved value, so the section never appears twice. `{_, false}` stays as it is. No bullets: put back the saved
    value. Otherwise put `{line, String.trim_trailing(doc) <> "\n\n" <> section}`, or `{1, section}` when no
    moduledoc exists. Use `Module.put_attribute/3` with the `{line, doc}` form.
  - The section render is private in `VerifiedPubSub.Docs`, under `put_moduledoc/1` per `LAYOUT-2`. Look up each
    topic with `VerifiedPubSub.Info.topic!/2`.
  - The `@moduledoc` of `VerifiedPubSub`: add a short `## Docs` section. It names the `docs: true` option and shows one
    bullet of the output.

- [ ] **Step 5: Run the tests.** Run: `mix test test/verified_pubsub/pubsub_docs_test.exs`. Expected: PASS.

- [ ] **Step 6: Verify and commit.** Run `mix format`, `mix compile --warnings-as-errors`, `mix test`. Commit:
  `Add a PubSub section to the moduledoc of a broadcaster`.

### Task 2: Subscribes section, and the Subscriber

**Files:**
- Modify: `lib/verified_pubsub/subscriber.ex` (`@options`, `__using__/1`, `__before_compile__/1`, moduledoc)
- Modify: `lib/verified_pubsub/api.ex` (`subscribe/2` records the topic)
- Modify: `lib/verified_pubsub/docs.ex` (Subscribes bullets)
- Test: `test/verified_pubsub/pubsub_docs_test.exs`

**Interfaces:**
- Consumes: `bind_docs!/2`, `Docs.put_moduledoc/1`, `fetch_moduledoc!/2` from Task 1.
- Produces: module attribute `@verified_pubsub_subscriptions`: entries `topic_name :: atom()`.

- [ ] **Step 1: Write the failing tests.**

  `describe "use VerifiedPubSub.Subscriber with docs on"`:

  - `"lists the handled events, and leaves out the ignored events"`. Body:

    ```elixir
    use VerifiedPubSub.Subscriber, manifest: VerifiedPubSub.TestManifests.Basic, docs: true

    handle_message :pets, [:adopted, :renamed], payload, state do
      broadcast!(:system, :alert, %{text: payload.id})
      {:noreply, state}
    end

    handle_message :pets, :rehomed, _payload, state do
      {:noreply, state}
    end

    ignore_message :system, :alert
    ```

    Expected moduledoc:

    ```text
    ## PubSub

      * Publishes to `"system"` for `:alert`.
      * Subscribes to `"owners:<owner_id>:pets"` for `:adopted`, `:rehomed`, and `:renamed`.
    ```

    This test also covers a broadcast in a `handle_message` body, and a topic with only ignored events.

  - `"lists no topic for a subscribe call"`: a Subscriber module that calls `subscribe(:system)` in a `def`, and only
    ignores `:pets` events and `:system` events. Returns `:none`.

  `describe "use VerifiedPubSub with docs on and a subscribe call"`:

  - `"lists each subscribed topic with no events"`: `def join, do: subscribe(:system)` and
    `def join(id), do: subscribe(:pets, %{owner_id: id})`. Expected:
    ``"## PubSub\n\n  * Subscribes to `\"owners:<owner_id>:pets\"`.\n  * Subscribes to `\"system\"`.\n"``.

  `describe "a module with both use lines"`:

  - `"gets one section when use VerifiedPubSub comes first"` and `"gets one section when use VerifiedPubSub.Subscriber
    comes first"`: `docs: true` on one line only. One `handle_message :system, :alert` with a body that calls
    `broadcast!(:system, :alert, ...)`, and `ignore_message :pets, [:adopted, :renamed, :rehomed]`. Expected, in both
    orders:
    ``"## PubSub\n\n  * Publishes to `\"system\"` for `:alert`.\n  * Subscribes to `\"system\"` for `:alert`.\n"``.
  - `"gets one section when use VerifiedPubSub appears twice"`: two equal `use VerifiedPubSub` lines with
    `docs: true`, one broadcast. Exactly one `## PubSub` in the moduledoc.

- [ ] **Step 2: Run the tests.** Run: `mix test test/verified_pubsub/pubsub_docs_test.exs`. Expected: FAIL.

- [ ] **Step 3: Implement.**
  - `Subscriber`: add `:docs` to `@options`. Call `VerifiedPubSub.bind_docs!/2` in `__using__/1` after
    `bind_manifest!/2`. In `__before_compile__/1`, add `VerifiedPubSub.Docs.put_moduledoc(__MODULE__)` as the last
    expression of the returned block, after the dispatch clauses. A code comment says why it must come last: the
    `handle_message` bodies expand inside the dispatch clauses.
  - `Api.subscribe/2`: `Module.put_attribute(__CALLER__.module, :verified_pubsub_subscriptions, topic_struct.name)`.
  - `Docs`: in a module that has the `:verified_pubsub_clauses` attribute (`Module.has_attribute?/2`), the Subscribes
    bullets come from the `topic` and `events` of each clause. Otherwise they come from
    `@verified_pubsub_subscriptions`, with no events. Ignored events never appear.
  - The `@moduledoc` of `VerifiedPubSub.Subscriber`: one paragraph on the `docs` option and on which events appear.

- [ ] **Step 4: Run the tests.** Run: `mix test test/verified_pubsub/pubsub_docs_test.exs`. Expected: PASS.

- [ ] **Step 5: Verify and commit.** Run `mix format`, `mix compile --warnings-as-errors`, `mix test`. Commit:
  `List the subscribed topics in the PubSub section`.

### Task 3: The manifest default, and the user docs

**Files:**
- Modify: `lib/verified_pubsub/manifest.ex` (`opt_schema`, `handle_opts/1`, moduledoc)
- Modify: `lib/verified_pubsub/docs.ex` (fall back to the manifest)
- Modify: `test/support/manifests.ex` (new `Documented` manifest)
- Modify: `README.md`, `usage-rules.md`, `CHANGELOG.md`
- Test: `test/verified_pubsub/pubsub_docs_test.exs`

**Interfaces:**
- Consumes: `bind_docs!/2` and `put_moduledoc/1` from Task 1.
- Produces: `MyManifest.__verified_pubsub_docs__() :: boolean()`, `@doc false`, defined by `handle_opts/1`.

- [ ] **Step 1: Add the fixture.** In `test/support/manifests.ex`, add `VerifiedPubSub.TestManifests.Documented` with
  `pubsub: VerifiedPubSub.TestPubSub, docs: true` and `topic :system, ["system"]` with `message :alert` and
  `field :text, :string`.

- [ ] **Step 2: Write the failing tests.**

  `describe "use VerifiedPubSub.Manifest with docs on"`:

  - `"turns on the section for each module that binds it"`: `use VerifiedPubSub, manifest: ...Documented`, no
    `docs` option, one alert broadcast. Expected ``"## PubSub\n\n  * Publishes to `\"system\"` for `:alert`.\n"``.
  - `"lets a use line turn the section off"`: the same with `docs: false`. Returns `:none`.

  `describe "a module with both use lines"`:

  - `"is a compile error when the lines give different docs values"`: `use VerifiedPubSub` with `docs: true` and
    `use VerifiedPubSub.Subscriber` with `docs: false`, both on `Basic`. `CompileHelper.compile_error/1` returns an
    `ArgumentError` whose message contains `"docs: true"` and `"docs: false"`.

  `describe "use VerifiedPubSub with docs on"` (add to the Task 1 describe):

  - `"is a compile error when docs is not a boolean"`: `docs: :yes`. `ArgumentError` with
    `"expected a boolean for :docs, got: :yes"`.

  The Task 1 test `"adds no section by default"` already covers a manifest with no `docs` option.

- [ ] **Step 3: Run the tests.** Run: `mix test test/verified_pubsub/pubsub_docs_test.exs`. Expected: FAIL on the
  manifest tests. The two error tests can already pass from Task 1. Keep them, because they pin the contract.

- [ ] **Step 4: Implement.**
  - `Manifest`: add `docs: [type: :boolean, default: false, doc: "..."]` to `opt_schema`. In `handle_opts/1`, define
    `def __verified_pubsub_docs__, do: unquote(Keyword.get(opts, :docs, false))` with `@doc false`, beside
    `__verified_pubsub_name__/0`. Add one paragraph on `docs` to the moduledoc.
  - `Docs.put_moduledoc/1`: when `@verified_pubsub_docs` is `nil`, use `manifest.__verified_pubsub_docs__()`. Read the
    manifest from `@verified_pubsub_manifest`.

- [ ] **Step 5: Run the tests.** Run: `mix test test/verified_pubsub/pubsub_docs_test.exs`. Expected: PASS.

- [ ] **Step 6: Update the user docs.**
  - `README.md`: after "Document your topics", add "Document your modules". Show `docs: true` on the manifest and on a
    `use` line, and the output for the README's own `MyApp.Pets` example. Say that ignored events do not appear, and
    that the section shows only the calls in the module itself.
  - `usage-rules.md`: add rules for the `docs` option. Turn it on in the manifest for a whole app. Do not write the
    section by hand. A broadcast in another module that this module calls does not appear.
  - `CHANGELOG.md`: add `## v0.3.0 (unreleased)` above v0.2.0, with one entry for the `docs` option and the section.

- [ ] **Step 7: Verify and commit.** Run `mix format`, `mix compile --warnings-as-errors`, `mix test`. Run `mix docs`
  and confirm that it gives no warnings. Commit:
  `Let a manifest turn on the PubSub section for its modules`.
