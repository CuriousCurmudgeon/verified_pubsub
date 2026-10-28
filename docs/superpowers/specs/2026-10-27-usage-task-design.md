# A mix task that reports topic usage — Design

**Date:** 2026-10-27
**Status:** Draft

## Purpose

A developer wants to know where a topic or an event is in use. Today they grep the code, or an LLM greps it for them.
Grep misses some call sites. For example, a `broadcast!` inside a `handle_message` body looks the same as any other
call, and the name of a topic is a bare atom that also occurs in unrelated code.

The macros of VerifiedPubSub already resolve each topic and event at compile time. With this change, the macros also
record each call site in the compiled module. A mix task reads those records and prints them, in the way that
`mix phx.routes` reads `__routes__/0` of a router.

The task answers three questions:

1. Which processes receive a broadcast of this event?
2. Which code breaks when this event changes or goes away?
3. How much code uses this topic or event? The user uses the counts to judge how important a topic or an event is.

## Goals

1. Each call of a VerifiedPubSub macro that names a topic records one entry for each event, with its `file:line`.
2. The entries persist in the `.beam` of the module that makes the call.
3. `mix verified_pubsub.topics` prints each call site of each topic of each manifest, grouped by event.
4. A topic argument limits the output to that topic. A topic argument and an event argument limit it to that event.
5. `--summary` prints the counts of each topic in the place of the call sites. The arguments limit it in the same way.
6. `--format json` prints the same data in a form that a program or an LLM can parse.

## Non-goals

- A runtime view. The task does not start the application, and it does not show live subscriptions.
- Calls that bypass VerifiedPubSub. A direct `Phoenix.PubSub.broadcast/3` call records nothing.
- Calls across modules. When module A calls a function of module B, and B broadcasts, the entry belongs to B.
- The modules of dependencies. The task reads only the modules of the current project.
- The moduledoc. The `pubsub-moduledoc` branch is an experiment, and this design does not depend on it.

## Anti-goals

- A count that disagrees with the code. Each entry comes from a macro call that the module compiled.
- A new cost at runtime. The macros record at expansion. The code that they generate does not change.

## What gets recorded

Each macro call records entries when it expands. An entry holds these values:

| Key | Value |
| --- | --- |
| `manifest` | The manifest module that the caller binds. |
| `topic` | The name of the topic, as `:pets`. |
| `event` | The name of the event, or `nil` for a call that takes no event. |
| `kind` | The kind of call. See the next table. |
| `module` | `__CALLER__.module`. |
| `file` | `__CALLER__.file`, as an absolute path. The task makes it relative to the root where it runs. (My call.) |
| `line` | `__CALLER__.line`. |

| Kind | Macros | Entries |
| --- | --- | --- |
| `:broadcast` | `broadcast/3`, `broadcast/4`, `broadcast!/3`, `broadcast!/4` | one |
| `:broadcast_from` | `broadcast_from/4`, `broadcast_from/5`, `broadcast_from!/4`, `broadcast_from!/5` | one |
| `:handle` | `handle_message/5`, `handle_message/6` | one for each event in the call |
| `:ignore` | `ignore_message/2` | one for each event in the call |
| `:subscribe` | `subscribe/1`, `subscribe/2` | one, with no event |
| `:unsubscribe` | `unsubscribe/1`, `unsubscribe/2` | one, with no event |

`topic/1` and `topic/2` record nothing. They only build a string.

The output shows the bang form and the plain form under one kind. The user did not ask to tell them apart. (My call.)

A `broadcast!` inside a `handle_message` body records an entry. The body expands later, inside the dispatch clauses
that `VerifiedPubSub.Subscriber.__before_compile__/1` returns. The entry must still carry the line of the `broadcast!`
in the source of the user. A test must cover this case.

### Where the entries live

`use VerifiedPubSub` and `use VerifiedPubSub.Subscriber` register an accumulating attribute with `persist: true`. They
register it during expansion, for the reason that the comment in `VerifiedPubSub.Subscriber.__using__/1` gives. When
a module has both `use` lines, the attribute must register once.

The macros record in every env, also in `prod`, and no option turns the record off. `__routes__/0` of a
`Phoenix.Router` also compiles into every env. The user chose this. The moduledoc does not list it as a tradeoff.

The task reads the attribute from each compiled module. It does not need to start the application. The builder picks
how to read it, from `module.__info__(:attributes)` or from the `:attributes` chunk of the `.beam`.

A macro call inside a shared `__using__`, as `use MyAppWeb, :live_view`, or inside a user macro, expands once in
each module that uses it. Each expansion records its own entry, with its own `module`. The entry points at the `use`
line of each module. When the quote has `location: :keep`, each entry points at the same line in the helper. A count is a number of entries, not of distinct source lines. A
`handle_message` with a list of two events also counts two. The count then measures how many places receive or send
the event. (My call.)

The shape of an entry is internal. A later version can change it. The JSON output is the stable form. (My call.)

## The task

The name is `mix verified_pubsub.topics`. (My call. The name follows the noun of `phx.routes`.)

The task runs `mix compile` first, as `phx.routes` does. Then it finds two sets of modules in the current project:

- The manifests. A manifest exports `__verified_pubsub_name__/0`.
- The modules with entries.

An entry can name a manifest that a dependency defines. The task loads that manifest and reports it with the others.
A manifest of a dependency with no entries in the project does not show. (My call.)

The task runs in the current `MIX_ENV`, as `phx.routes` does. Thus call sites in `test/support` show only with
`MIX_ENV=test`. An `.exs` file records nothing, because the task does not compile it. The moduledoc of the task and
`usage-rules.md` state this gap. (My call.)

The task joins the entries to the manifests. The manifest gives the order of the topics and the events, the wire
pattern of each topic, and the events that no call site uses. `VerifiedPubSub.Info` already reads the topics of a
manifest.

In an umbrella project, the task at the root reads the modules of every child app together. A manifest in one app can
serve modules in another app, so a report for each app alone gives counts that are too low. (My call.)

### Arguments and options

```text
mix verified_pubsub.topics [TOPIC [EVENT]] [--summary] [--manifest MODULE] [--format text|json]
```

The arguments choose what the output holds, and `--summary` chooses how much detail it gives. The user chose this,
so that each filter works in each view, as `mix phx.routes` prints each route by default.

- `TOPIC` is the name of a topic without the colon, as `pets`.
- `EVENT` is the name of an event on that topic, as `adopted`. An event is an argument and not an option, because two
  topics can declare events with the same name. (My call.)
- `--manifest` limits the output to one manifest. When `TOPIC` matches topics in more than one manifest, the task
  prints each match.
- `--summary` prints the summary table. Without it, the task prints the detail view.
- `--format` defaults to `text`.

The task compares each argument with `Atom.to_string/1` of each declared name. It must not call `String.to_atom/1` on
an argument.

An unknown topic or event is an error. The message lists the valid names, as the compile errors of the macros do. The
task exits with a non-zero status.

### Text output: the summary

The summary prints one table for each manifest, with one row for each topic, in the order of the manifest. Each
count is a number of call sites.

```text
MyApp.Topics

Topic     Wire topic                  Events  Broadcasts  Handlers  Subscribes
:pets     owners:<owner_id>:pets           2           5         3           2
:treats   owners:<owner_id>:treats         1           1         0           1
```

- `Broadcasts` counts the entries of `:broadcast` and `:broadcast_from` on the topic.
- `Handlers` counts the entries of `:handle` on the topic.
- `Subscribes` counts the entries of `:subscribe` on the topic.
- `:ignore` and `:unsubscribe` entries do not show in the summary. (My call. They show in the detail view.)
- A topic argument keeps only the row of that topic. An event argument also limits the counts of that row to the
  event and the subscribes of the topic.

### Text output: the detail view

The detail view prints the name of each manifest, then each topic that the arguments keep, as this example of one
topic shows. A blank line separates two topics, and two manifests. The user chose to group the topics under their
manifest, as the summary does.

```text
MyApp.Topics

:pets  owners:<owner_id>:pets

Subscribes (2)
  subscribe        lib/my_app_web/live/pets_live.ex:14  MyAppWeb.PetsLive
  subscribe        lib/my_app/notifier.ex:22            MyApp.Notifier

:adopted  3 broadcasts, 2 handlers
  broadcast        lib/my_app/pets.ex:12                MyApp.Pets
  broadcast        lib/my_app/imports.ex:40             MyApp.Imports
  broadcast_from   lib/my_app_web/live/pets_live.ex:51  MyAppWeb.PetsLive
  handle_message   lib/my_app_web/live/pets_live.ex:30  MyAppWeb.PetsLive
  handle_message   lib/my_app/notifier.ex:30            MyApp.Notifier

:rehomed  0 broadcasts, 1 handler, 1 ignore  (no broadcast)
  handle_message   lib/my_app/notifier.ex:34            MyApp.Notifier
  ignore_message   lib/my_app_web/live/pets_live.ex:40  MyAppWeb.PetsLive
```

- The `Subscribes` group lists the `:subscribe` and `:unsubscribe` entries of the topic. A topic with no entry there
  gets the one line `Subscribes: none`.
- The events follow the order of the manifest. Each event shows, also an event with no entries.
- Within an event, broadcasts come first, then handlers, then ignores. Within each kind, entries sort by file and
  line.
- An event with no broadcast gets the flag `(no broadcast)`. A handler of that event never runs. (My call.)
- The header of an event leaves out a kind with a count of zero, except for broadcasts. (My call.)
- When ANSI is on, the text has color, and in a pipe it has none. The user asked for color:
  - The name of a manifest, the name of an event, and the header row of the summary are bold. The name of a topic is
    cyan.
  - A send (`broadcast`, `broadcast_from`) is green, and a receive (`handle_message`, `subscribe`) is magenta.
    `ignore_message` and `unsubscribe` are faint.
  - `(no broadcast)` is yellow. A `file:line` has no color, so that a reader can copy it.

### Text output: one event

The output holds the header of the topic, the `Subscribes` group, and the block of that one event. The subscribers
answer the first question of the [Purpose](#purpose), so they show here too.

### JSON output

`--format json` prints one JSON document. It holds the same data as the text output for the same arguments, and the
same filters apply. A sketch of the shape:

```json
{
  "manifests": [
    {
      "module": "MyApp.Topics",
      "topics": [
        {
          "name": "pets",
          "wire": "owners:<owner_id>:pets",
          "subscribes": [{"kind": "subscribe", "module": "MyAppWeb.PetsLive", "file": "lib/...", "line": 14}],
          "events": [
            {
              "name": "adopted",
              "counts": {"broadcast": 3, "handle": 2, "ignore": 0},
              "call_sites": [{"kind": "broadcast", "module": "MyApp.Pets", "file": "lib/my_app/pets.ex", "line": 12}]
            }
          ]
        }
      ]
    }
  ]
}
```

- Module names are strings without the `Elixir.` prefix. Topic and event names are strings without the colon.
- The output is compact, on one line. A reader who wants indentation pipes it to `jq`. The user chose this, because the
  main reader is a program or an LLM, and the `JSON` module of Elixir does not indent.
- The keys of each object come in a fixed order, so that a reader sees what an object is before its contents:
  - a manifest: `module`, `topics`
  - a topic: `name`, `wire`, `counts`, `subscribes`, `events`
  - an event: `name`, `counts`, `call_sites`
  - a call site: `kind`, `file`, `line`, `module`
  - the counts: `broadcast`, `handle`, `ignore`, `subscribe`
- The summary in JSON holds the counts of each topic and each event, and leaves out `call_sites` and `subscribes`.
- An event has no `subscribe` count. A subscribe names a topic and no event, so the count would always be 0.
- The library needs Elixir 1.16 or later, and the `JSON` module arrives in Elixir 1.18. On an older Elixir,
  `--format json` is an error that names Elixir 1.18. The library adds no dependency for JSON. (My call.)

## Tests

Follow the patterns of `test/verified_pubsub/docs_test.exs` and `test/mix/tasks/verified_pubsub.install_test.exs`.

Cover at least:

- each kind of entry, with its file and line
- a `handle_message` and an `ignore_message` with a list of events, which give one entry for each event
- a `broadcast!` inside a `handle_message` body, with the line of the source
- a module with both `use` lines
- the summary counts, and the order of the manifest
- the detail view of each topic without an argument, and `--summary` with a topic and an event
- one topic, one event, and an event with no broadcast
- an event name that two topics share
- `--manifest`, and a topic name in two manifests
- an unknown topic and an unknown event, which exit non-zero and list the valid names
- `--format json` for the summary and the detail view
- a manifest with no call sites

## Docs to update

Per `AGENTS.md`: `README.md`, `usage-rules.md`, and `CHANGELOG.md`. `usage-rules.md` tells an LLM to run
`mix verified_pubsub.topics TOPIC --format json` before it greps for a topic. The task takes a `@shortdoc` and a
`@moduledoc` that lists its arguments and options.

## Left to the builder

- The name of the persisted attribute, and the shape of an entry.
- How the task reads the attribute.
- How the task finds the modules of the project, and the modules of an umbrella.
- The column widths and the alignment of the text output.
- The module layout, as one task module or a task and a module that builds the report.
