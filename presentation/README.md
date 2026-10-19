# Verified PubSub — ElixirConf 2026 lightning talk

A [reveal.js](https://revealjs.com) deck. The speaker notes hold the script, so you do not
need the Obsidian doc that it came from to present.

## Present the deck

```sh
./presentation/present.sh
```

Then, with the deck focused:

1. Press **S**. A second window opens with the notes, the current slide, the next slide,
   and a timer.
2. Move that window to the screen that only you can see.
3. Put the deck window on the projector and press **F** for full screen.

The two windows stay in sync, and either one can control the deck.

JavaScript opens the notes window. If the browser blocks it the first time, allow pop-ups
for `127.0.0.1`.

When you open `index.html` directly from disk, the slides show but the notes window does
**not**. The notes window needs an HTTP origin, and `present.sh` gives one.

## Layout

| Path | What it is |
| --- | --- |
| `index.html` | The slides, with the script in `<aside class="notes">` for each slide |
| `theme.css` | White slides, black text, dark code panels |
| `vendor/` | reveal.js, its notes plugin, and highlight.js with the Elixir grammar |

The repo holds a copy of each dependency, so the deck works without a network. That is
the purpose, at a conference.

## Edit the slides

The slides are plain HTML in `index.html`. Each `<section>` is one slide, and its
`<aside class="notes">` shows in the notes window. The code blocks have the indent of the
markup around them, and the deck removes that indent when it loads. So paste code at the
indent that reads well in the file.
