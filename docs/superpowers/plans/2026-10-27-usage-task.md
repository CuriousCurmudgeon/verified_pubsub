# Topics Mix Task Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mix verified_pubsub.topics` prints where each topic and event of each manifest is in use, with
`file:line`, as text or as JSON.

**Architecture:** Each macro of the API and of the Subscriber records one `VerifiedPubSub.Usage.Entry` for each event
in a persisted, accumulating module attribute while it expands. `VerifiedPubSub.Usage.Report.build/2` reads the
entries of a list of modules and joins them to the manifests. It applies the filters, and it returns one report.
`VerifiedPubSub.Usage.Text` and `VerifiedPubSub.Usage.Json` render the report. The mix task parses the arguments,
compiles, finds the modules of the project, and prints the result.

**Tech Stack:** Elixir 1.16+, Mix, ExUnit, the `JSON` module of Elixir 1.18+ (optional at runtime).

**Spec:** `docs/superpowers/specs/2026-10-27-usage-task-design.md`

## Global Constraints

- Branch: `usage-task`. Do not change `@version` in `mix.exs`.
- The attribute name is `:verified_pubsub_usage`. Register it with `accumulate: true, persist: true`.
- The macros record in every env. No option turns the record off.
- The kinds are exactly `:broadcast`, `:broadcast_from`, `:handle`, `:ignore`, `:subscribe`, `:unsubscribe`.
- The text output names each kind by its macro: `broadcast`, `broadcast_from`, `handle_message`, `ignore_message`,
  `subscribe`, `unsubscribe`.
- Never call `String.to_atom/1` or `Module.concat/1` on an argument of the task. Compare strings.
- The library adds no dependency. CI runs Elixir 1.16, so no source file may name `JSON` as a remote call that the
  compiler checks. Call it through a variable, as `json = JSON` then `json.encode!(data)`, so that
  `mix compile --warnings-as-errors` passes on 1.16.
- New internal modules take `@moduledoc false`. The mix task takes a `@shortdoc` and a full `@moduledoc`.
- Follow `AGENTS/STYLE.md` and `AGENTS/DOCS.md`: STE prose in docs, comments, test names, and commit messages. Test
  names follow `SPEC-1` to `SPEC-3`. Let logs print (`TEST-1`).
- After each task, run in order: `mix format`, `mix compile --warnings-as-errors`, `mix test`. Fix each issue.
- End each commit message with `Assisted-By: Claude Opus 5.5 <noreply@anthropic.com>`. Do not push.

## Review Focus

1. A topic or event argument with a leading colon, as `:pets`. The task treats it as `pets`. Test in Task 2.
2. A `broadcast!` inside a `__using__` quote that two modules use. Each module gets its own entry. Test in Task 1.
3. A module that uses both `VerifiedPubSub` and `VerifiedPubSub.Subscriber`. Its entries do not double, and the
   attribute registers once. Test in Task 1.
4. A `--manifest` value that names no manifest. The error lists the manifests that exist. Test in Task 2.
5. A project with no manifest. The overview prints one line that says so, and the task exits with status 0. Test in
   Task 3.

---

### Task 1: Record each call site

**Files:**
- Create: `lib/verified_pubsub/usage.ex` (`VerifiedPubSub.Usage`, `VerifiedPubSub.Usage.Entry`)
- Modify: `lib/verified_pubsub.ex` (`bind_manifest!/2` registers the attribute in its `nil` branch)
- Modify: `lib/verified_pubsub/api.ex` (`subscribe/2`, `unsubscribe/2`, `build/7` record)
- Modify: `lib/verified_pubsub/subscriber.ex` (`put_clause!/7`, `ignore_message/2` record)
- Test: `test/verified_pubsub/usage_test.exs`

**Interfaces:**
- Produces:
  - `%VerifiedPubSub.Usage.Entry{manifest: module(), topic: atom(), event: atom() | nil, kind: kind(),
    module: module(), file: String.t(), line: pos_integer()}`. `@typedoc` of `t:t/0` lists the fields. `kind/0` is a
    `@type` whose `@typedoc` lists the six kinds.
  - `VerifiedPubSub.Usage.register!(module()) :: :ok`
  - `VerifiedPubSub.Usage.record!(Macro.Env.t(), module(), atom(), atom() | nil, Entry.kind()) :: :ok`. It takes
    the caller, the manifest, the topic, the event, and the kind. It reads `module`, `file`, and `line` from the
    caller. `file` stays as `caller.file`.
  - `VerifiedPubSub.Usage.entries(module()) :: [Entry.t()]`. It returns `[]` for a module with no entries.

- [ ] **Step 1: Write the failing tests**

Compile each module with `VerifiedPubSub.CompileHelper.compile!/1` against `VerifiedPubSub.TestManifests.Basic`. Give
each test module a name from `unique_module/1`. Find each line from the source string, so that the assertion does not
hard-code a number. Use these `describe` names and `test` names:

- `broadcast/4, broadcast!/4, broadcast/3, broadcast!/3`: `"records a :broadcast entry with the topic, the event, the
  module, the file, and the line"`. Assert all seven fields of the entry for `broadcast!(:pets, %{owner_id: "1"},
  :adopted, %{id: "1"})` and for `broadcast!(:system, :alert, %{text: "x"})`.
- `broadcast_from/5, broadcast_from!/5, broadcast_from/4, broadcast_from!/4`: `"records a :broadcast_from entry"`.
- `subscribe/2, subscribe/1`: `"records a :subscribe entry with no event"`. Assert `event: nil`.
- `unsubscribe/2, unsubscribe/1`: `"records an :unsubscribe entry with no event"`.
- `topic/2`: `"records no entry"`.
- `handle_message/5`: `"records one :handle entry for each event in the list"`. Assert that
  `handle_message :pets, [:adopted, :renamed], ...` gives two entries on the same line, in that order.
- `handle_message/5 with a broadcast in the body`: `"records the broadcast with the line of the body"`. Assert that
  the line is the line of the `broadcast!` in the source, not the line of `handle_message`.
- `ignore_message/2`: `"records one :ignore entry for each event in the list"`.
- `use VerifiedPubSub, use VerifiedPubSub.Subscriber`: `"records each call once in a module with both use lines"`.
  Use both `use` lines, one `broadcast!` and one `handle_message`. Assert exactly two entries.
- `a broadcast in a shared __using__`: `"records one entry for each module that uses it"`. Define a helper module
  whose `__using__/1` quote holds `use VerifiedPubSub, manifest: VerifiedPubSub.TestManifests.Basic` and a `def`
  that calls `broadcast!`. Compile two modules that use it. Assert one entry in each, with the same `file` and
  `line`, and a different `module`.
- `VerifiedPubSub.Usage.entries/1`: `"returns an empty list for a module that binds no manifest"`. Use `Enum`.

- [ ] **Step 2: Run the tests and see them fail**

Run: `mix test test/verified_pubsub/usage_test.exs`
Expected: FAIL, because `VerifiedPubSub.Usage` is not defined.

- [ ] **Step 3: Implement `VerifiedPubSub.Usage` and the records**

- `register!/1` calls `Module.register_attribute/3`. `VerifiedPubSub.bind_manifest!/2` calls it only in its `nil`
  branch, so a second `use` does not register again. Keep the reason of the comment in
  `VerifiedPubSub.Subscriber.__using__/1`: the register must run at expansion, not in a quote.
- `entries/1` reads `module.__info__(:attributes)`. Elixir stores each value of a persisted, accumulating attribute
  under the same key, so collect every value of `:verified_pubsub_usage`. Return them in source order.
- In `api.ex`, record after `resolve!/2` in `subscribe/2` and `unsubscribe/2`. Record in `build/7` after
  `validate_event!/4`, with `:broadcast_from` when `from` is not `nil`, else `:broadcast`.
- In `subscriber.ex`, record in `put_clause!/7` after the validations, once for each event. Record in
  `ignore_message/2` once for each event. Read the manifest from `:verified_pubsub_manifest` of the caller.

- [ ] **Step 4: Run the tests and see them pass**

Run: `mix test test/verified_pubsub/usage_test.exs`, then `mix test`
Expected: PASS. The other test files do not change.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/usage.ex lib/verified_pubsub.ex lib/verified_pubsub/api.ex \
  lib/verified_pubsub/subscriber.ex test/verified_pubsub/usage_test.exs
git commit -m "Record each call site of a topic in the compiled module" -m "Assisted-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Build the report

**Files:**
- Create: `lib/verified_pubsub/usage/report.ex`
- Test: `test/verified_pubsub/usage/report_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubSub.Usage.entries/1`, `VerifiedPubSub.Usage.Entry`, `VerifiedPubSub.Info.topics/1`,
  `VerifiedPubSub.Topic.pattern/1`, `VerifiedPubSub.Topic.listing/1`.
- Produces:
  - `%VerifiedPubSub.Usage.Report{view: :overview | :topic | :event, manifests: [Report.Manifest.t()]}`
  - `%VerifiedPubSub.Usage.Report.Manifest{module: module(), topics: [Report.Topic.t()]}`
  - `%VerifiedPubSub.Usage.Report.Topic{name: atom(), wire: String.t(), subscribes: [Entry.t()],
    events: [Report.Event.t()]}`
  - `%VerifiedPubSub.Usage.Report.Event{name: atom(), entries: [Entry.t()]}`
  - `VerifiedPubSub.Usage.Report.build([module()], keyword()) :: {:ok, t()} | {:error, String.t()}`. The options are
    `manifest: String.t()`, `topic: String.t()`, and `event: String.t()`. Each defaults to `nil`.
  - `VerifiedPubSub.Usage.Report.counts([Entry.t()]) :: counts()`. `counts/0` is a map with the keys `:broadcast`,
    `:handle`, `:ignore`, and `:subscribe`. `:broadcast` counts `:broadcast` and `:broadcast_from` entries.
    `:subscribe` counts only `:subscribe` entries.

- [ ] **Step 1: Write the failing tests**

Build each report from modules that `compile!/1` compiles in the test, plus `VerifiedPubSub.TestManifests.Basic`.
Define an inline manifest in a test when it needs a second manifest or a shared event name.

- `build/2`:
  - `"lists each manifest with its topics and events in the order of the manifest"`. Assert the order of the topic
    names and the event names of `Basic`, and `view: :overview`.
  - `"gives each topic its wire pattern"`. Assert `wire: "owners:<owner_id>:pets"`.
  - `"puts the subscribe and unsubscribe entries in the subscribes of the topic"`.
  - `"orders the entries of an event by broadcast, broadcast_from, handle, and ignore, then by file and line"`.
  - `"makes each file relative to the current directory"`. Compile with `Code.compile_string(source,
    Path.join(File.cwd!(), "lib/fake.ex"))`. Assert `file: "lib/fake.ex"`.
  - `"reports a manifest that only an entry names"`. Pass only the caller module, not the manifest. Assert that the
    manifest is in the report.
  - `"leaves out a manifest that no module of the list defines and no entry names"`.
- `build/2 with a topic`:
  - `"keeps only the topic, in the :topic view"`.
  - `"accepts the topic with a leading colon"`. Assert that `topic: ":pets"` and `topic: "pets"` give equal reports.
  - `"keeps the topic of each manifest that declares it"`.
  - `"returns an error that lists the declared topics for an unknown topic"`. Assert that the message holds
    `unknown topic :petz` and the output of `VerifiedPubSub.Topic.listing/1`.
- `build/2 with a topic and an event`:
  - `"keeps only the event and the subscribes of the topic, in the :event view"`.
  - `"keeps the event of the named topic when another topic declares an event with the same name"`.
  - `"returns an error that lists the events of the topic for an unknown event"`. Assert that the message holds
    `unknown event :adoptd on topic :pets (owners:<owner_id>:pets)` and each declared event.
- `build/2 with a manifest`:
  - `"keeps only the named manifest"`. Pass `manifest: "VerifiedPubSub.TestManifests.Basic"`.
  - `"returns an error that lists the manifests for an unknown manifest"`.
- `counts/1`: `"counts broadcast and broadcast_from as broadcasts"`.

- [ ] **Step 2: Run the tests and see them fail**

Run: `mix test test/verified_pubsub/usage/report_test.exs`
Expected: FAIL, because `VerifiedPubSub.Usage.Report` is not defined.

- [ ] **Step 3: Implement `VerifiedPubSub.Usage.Report.build/2` and `counts/1`**

A manifest is a module that exports `__verified_pubsub_name__/0`. Call `Code.ensure_loaded/1` before
`function_exported?/3`. Sort the manifests by `inspect/1`. Strip one leading `:` from the topic and the event, then
compare each with `Atom.to_string/1` of the declared names. When a topic matches in many manifests, an unknown event
is an error only when no matched topic declares it.

- [ ] **Step 4: Run the tests and see them pass**

Run: `mix test test/verified_pubsub/usage/report_test.exs`, then `mix test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/usage/report.ex test/verified_pubsub/usage/report_test.exs
git commit -m "Build a report of the call sites of each topic" -m "Assisted-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Render the report as text

**Files:**
- Create: `lib/verified_pubsub/usage/text.ex`
- Test: `test/verified_pubsub/usage/text_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubSub.Usage.Report` and its structs, `VerifiedPubSub.Usage.Report.counts/1`.
- Produces: `VerifiedPubSub.Usage.Text.render(Report.t()) :: String.t()`

The format, from the spec:

- The overview prints, for each manifest, `inspect(module)`, a blank line, then a table. Put a blank line between two
  manifests. The headers are `Topic`, `Wire topic`, `Events`, `Broadcasts`, `Handlers`, `Subscribes`. Two spaces
  separate the columns. A text column pads on the right to its widest cell. A number column pads on the left to the
  width of its header. A topic shows as `inspect(name)`.
- With no manifest, the overview is exactly `No VerifiedPubSub manifest is in this project.`
- A topic view prints, for each topic, the line `<inspect(name)>  <wire>  (<inspect(manifest)>)`, a blank line, and
  the line `Subscribes (<n>)` with one entry line for each subscribe and unsubscribe entry. Then each event follows
  after a blank line. The event view prints the same, with one event.
- An event block starts with `<inspect(name)>  <n> broadcast(s)`. Then add `, <n> handler(s)` and `, <n> ignore(s)`
  only when the count is not zero. Use the singular for 1. Add `  (no broadcast)` when the broadcast count is 0.
- An entry line is `"  " <> String.pad_trailing(macro, 15) <> "  " <> String.pad_trailing("file:line", width) <> "  "
  <> inspect(module)`. `width` is the widest `file:line` in the whole view.
- Two topics in a view, from two manifests, have a blank line between them.

- [ ] **Step 1: Write the failing tests**

Build each `%Report{}` by hand in the test, so that the files and the lines are fixed. Assert the whole string with a
heredoc.

- `render/1`:
  - `"renders the overview as one table for each manifest"`. Use two manifests.
  - `"renders a line that says the project has no manifest"`.
  - `"renders a topic with its subscribes and each event"`. Use the example of the spec, with `:adopted` and
    `:rehomed`.
  - `"renders one event with the subscribes of its topic"`.
  - `"flags an event with no broadcast"`.
  - `"uses the singular for a count of one"`.

- [ ] **Step 2: Run the tests and see them fail**

Run: `mix test test/verified_pubsub/usage/text_test.exs`
Expected: FAIL, because `VerifiedPubSub.Usage.Text` is not defined.

- [ ] **Step 3: Implement `VerifiedPubSub.Usage.Text.render/1`**

- [ ] **Step 4: Run the tests and see them pass**

Run: `mix test test/verified_pubsub/usage/text_test.exs`, then `mix test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/usage/text.ex test/verified_pubsub/usage/text_test.exs
git commit -m "Render the report of the call sites as text" -m "Assisted-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Render the report as JSON

**Files:**
- Create: `lib/verified_pubsub/usage/json.ex`
- Modify: `test/test_helper.exs` (exclude the `:json` tag when `JSON` is not loaded)
- Test: `test/verified_pubsub/usage/json_test.exs`

**Interfaces:**
- Consumes: `VerifiedPubSub.Usage.Report` and its structs, `VerifiedPubSub.Usage.Report.counts/1`.
- Produces: `VerifiedPubSub.Usage.Json.render(Report.t()) :: {:ok, String.t()} | {:error, String.t()}`. The error
  is `--format json needs Elixir 1.18 or later, because it uses the JSON module of Elixir.`

The shape, from the spec:

- The top level is `{"manifests": [...]}`. A manifest is `{"module", "topics"}`.
- A topic is `{"name", "wire", "counts", "events"}`. In the `:topic` and `:event` views it also holds `"subscribes"`.
- An event is `{"name", "counts"}`. In the `:topic` and `:event` views it also holds `"call_sites"`.
- `"counts"` holds the keys of `counts/0`. The counts of a topic come from its subscribes and the entries of each
  event.
- A call site and a subscribe are `{"kind", "module", "file", "line"}`. `"kind"` is `Atom.to_string(kind)`.
- A module is `inspect(module)`. A topic name and an event name are `Atom.to_string(name)`.

- [ ] **Step 1: Write the failing tests**

Tag the module `@moduletag :json`. Build each `%Report{}` by hand. Decode the output with `JSON.decode!/1` and assert
on the map.

- `render/1`:
  - `"renders the overview with counts and no call sites"`.
  - `"renders a topic with its subscribes and the call sites of each event"`.
  - `"renders one event"`.
  - `"writes a module without the Elixir prefix and a name without the colon"`.

- [ ] **Step 2: Run the tests and see them fail**

Run: `mix test test/verified_pubsub/usage/json_test.exs`
Expected: FAIL, because `VerifiedPubSub.Usage.Json` is not defined.

- [ ] **Step 3: Implement `VerifiedPubSub.Usage.Json.render/1`**

Check `Code.ensure_loaded?(JSON)`, and call `encode!/1` through a variable, as the Global Constraints say.

- [ ] **Step 4: Run the tests and see them pass**

Run: `mix test test/verified_pubsub/usage/json_test.exs`, then `mix compile --warnings-as-errors`, then `mix test`
Expected: PASS, with no warning.

- [ ] **Step 5: Commit**

```bash
git add lib/verified_pubsub/usage/json.ex test/test_helper.exs test/verified_pubsub/usage/json_test.exs
git commit -m "Render the report of the call sites as JSON" -m "Assisted-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The mix task and its docs

**Files:**
- Create: `lib/mix/tasks/verified_pubsub.topics.ex` (`Mix.Tasks.VerifiedPubsub.Topics`)
- Create: `test/support/usage_fixtures.ex` (a manifest, a broadcaster, and a subscriber)
- Test: `test/mix/tasks/verified_pubsub.topics_test.exs`
- Modify: `README.md`, `usage-rules.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `VerifiedPubSub.Usage.Report.build/2`, `VerifiedPubSub.Usage.Text.render/1`,
  `VerifiedPubSub.Usage.Json.render/1`.
- Produces: `mix verified_pubsub.topics [TOPIC [EVENT]] [--manifest MODULE] [--format text|json]`.

- [ ] **Step 1: Write the failing tests**

The test env compiles `test/support` into the `:verified_pubsub` app, so the task finds the fixtures. Pass
`--manifest` with the fixture manifest in each test, so that the other manifests of `test/support` stay out. Set
`Mix.shell(Mix.Shell.Process)` in `setup`, and read the output with `assert_received {:mix_shell, :info, [output]}`.

- `mix verified_pubsub.topics`:
  - `"prints the overview of the manifest"`. Assert the row of each fixture topic.
  - `"prints each call site of a topic"`. Assert that the output holds `test/support/usage_fixtures.ex:` and each
    fixture module.
  - `"prints one event"`.
  - `"prints JSON with --format json"`. Tag it `:json`. Decode the output.
  - `"raises for an unknown topic"`. Use `assert_raise Mix.Error, ~r/unknown topic :petz/`.
  - `"raises for an unknown format"`. The message names `text` and `json`.
  - `"raises for more than two arguments"`. The message shows the usage line.

- [ ] **Step 2: Run the tests and see them fail**

Run: `mix test test/mix/tasks/verified_pubsub.topics_test.exs`
Expected: FAIL, because the task `verified_pubsub.topics` does not exist.

- [ ] **Step 3: Implement `Mix.Tasks.VerifiedPubsub.Topics.run/1`**

- Parse with `OptionParser.parse!/2` and `strict: [manifest: :string, format: :string]`.
- Run `Mix.Task.run("compile")`.
- The modules are `Application.spec(app, :modules)` for each app of the project. An umbrella has the keys of
  `Mix.Project.apps_paths/0`, and another project has `Mix.Project.config()[:app]`. Call `Application.load/1` first.
- Print with `Mix.shell().info/1`. Raise each error with `Mix.raise/1`, which exits with a non-zero status.
- The `@moduledoc` lists the arguments and the options, states that the task runs in the current `MIX_ENV`, and
  states that an `.exs` file records nothing.

- [ ] **Step 4: Run the tests and see them pass**

Run: `mix test test/mix/tasks/verified_pubsub.topics_test.exs`, then `mix test`
Expected: PASS.

- [ ] **Step 5: Run the task by hand**

Run: `MIX_ENV=test mix verified_pubsub.topics --manifest VerifiedPubSub.UsageFixtures.Topics`. Use the module name
of the fixture manifest.
Expected: the overview table of the fixture manifest. Then add a topic argument, and expect the call sites.

- [ ] **Step 6: Update the docs**

- `README.md`: add a section `## Find where a topic is in use` after `## Subscribe`. Show the overview command and a
  topic command with short output, and `--format json`.
- `usage-rules.md`: add a section `## Find where a topic is in use`. Tell an agent to run
  `mix verified_pubsub.topics TOPIC --format json` before it greps for a topic or an event. Say that call sites in
  `test/support` show only with `MIX_ENV=test`.
- `CHANGELOG.md`: add `## Unreleased` above `## v0.2.0` with one item for the task and one for the record of call
  sites in each compiled module.

- [ ] **Step 7: Run the checks**

Run: `mix format`, `mix compile --warnings-as-errors`, `mix test`
Expected: no change from the format, no warning, and PASS.

- [ ] **Step 8: Commit**

```bash
git add lib/mix/tasks/verified_pubsub.topics.ex test/support/usage_fixtures.ex \
  test/mix/tasks/verified_pubsub.topics_test.exs README.md usage-rules.md CHANGELOG.md
git commit -m "Add mix verified_pubsub.topics" -m "Assisted-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
