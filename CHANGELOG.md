# Changelog

## v0.1.0 (unreleased)

The first release.

- `VerifiedPubSub.Manifest` declares topics, their events, and the fields each event's
  payload carries.
- `use VerifiedPubSub` adds `broadcast!`, `broadcast_from!`, `subscribe` and friends,
  which check topics, params, events and literal payloads at compile time. Payloads
  built at runtime are checked on every broadcast.
- `use VerifiedPubSub.Subscriber` adds `handle_message` and `ignore_message`, and fails
  compilation when a subscriber does not account for every event on the topics it
  subscribes to.
- `mix igniter.install verified_pubsub` adds the formatter import, finds or starts a
  `Phoenix.PubSub`, and creates an empty manifest.
- `usage-rules.md` for coding agents.
- A `description` option on `topic`, `message` and `field`. The manifest's `@moduledoc`
  gains a generated `## Topics` section with each topic's wire pattern, events, fields and
  descriptions.
