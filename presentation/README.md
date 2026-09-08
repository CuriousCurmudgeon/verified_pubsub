# Verified PubSub — ElixirConf 2026 lightning talk

A [reveal.js](https://revealjs.com) deck. The script lives in the speaker notes, so the
Obsidian doc it came from is not needed to present.

## Presenting

```sh
./presentation/present.sh
```

Then, with the deck focused:

1. Press **S**. A second window opens with the notes, the current slide, the upcoming
   slide, and a timer.
2. Drag that window to the screen only you can see.
3. Put the deck window on the projector and press **F** for fullscreen.

The two windows stay in sync, and either one can drive.

The notes window is opened by JavaScript, so allow pop-ups for `127.0.0.1` if the browser
blocks it the first time.

Opening `index.html` directly from disk renders the slides but **not** the notes window —
it needs an HTTP origin, which is what `present.sh` provides.

## Layout

| Path | What it is |
| --- | --- |
| `index.html` | The slides, with the script in `<aside class="notes">` per slide |
| `theme.css` | White slides, black text, dark code panels |
| `vendor/` | reveal.js, its notes plugin, and highlight.js with the Elixir grammar |

Everything is vendored, so the deck works with no network — which is the point, at a
conference.

## Editing

Slides are plain HTML in `index.html`; each `<section>` is one slide and its
`<aside class="notes">` is what shows up in the notes window. Code blocks are indented to
match the surrounding markup and de-indented at load time, so paste code in at whatever
indentation reads well in the file.
