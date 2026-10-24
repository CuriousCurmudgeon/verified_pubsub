# Changelog

## v0.1.0 (2026-10-19)

The first release.

- `VerifiedPubSub.Manifest` declares topics, their events, and the fields of the payload
  of each event.
- `use VerifiedPubSub` adds `broadcast!`, `broadcast_from!`, `subscribe` and the other
  macros of the API. They check topics, params, events and literal payloads at compile
  time. Each broadcast checks a payload that a call site builds at runtime.
- `use VerifiedPubSub.Subscriber` adds `handle_message` and `ignore_message`. A subscriber
  that does not handle or ignore every event on its topics is a compile error.
- `mix igniter.install verified_pubsub` adds the formatter import, finds or starts a
  `Phoenix.PubSub`, and creates an empty manifest.
- `usage-rules.md` gives rules for coding agents.
- The manifest documents itself. Its `@moduledoc` gets a generated `## Topics` section
  with the wire pattern, the events and the fields of each topic. `topic`, `message` and
  `field` take a `description` that says what each one means.
- VerifiedPubSub requires Elixir 1.16 or later.
