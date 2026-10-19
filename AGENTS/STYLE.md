Styleguide
==========

## Rule format

Each section opens with a principle. Each rule applies that principle to one case. When no rule covers a case, apply
the principle. A rule applies to new code. Leave existing code in place until you change it for another reason.

```text
### AREA-n

When <the condition holds>, <do this>. <Then do this>.[^note]
```

`AREA` is the code of the section, and `n` is the number of the rule in that section. The `When` clause states the
trigger. Each sentence after it gives one instruction. A footnote holds the reason. Wrap every line at 120 columns.

When prose names a rule, link to its heading with a lower case anchor.

```text
[`AREA-n`](#area-n)
```

## PROSE - Plain prose

Prose follows ASD-STE100 Simplified Technical English, so that a model and a person read the same meaning. Prose is a
code comment, a doc attribute, a test name, a Markdown file, a commit message, or a pull request description. Text
that a tool writes, such as the `usage-rules` block of `AGENTS.md`, is not prose.

### PROSE-1

When you write prose, obey these limits:

- Use the active voice and the simple present tense.
- Write one instruction per sentence. Use at most 20 words for an instruction and 25 words for a description.
- Keep one meaning for each word, and one word for each meaning.
- Do not use an `-ing` form as a noun or as part of a verb.
- Keep the articles `a`, `an`, and `the`.
- Use a plain word. Do not use slang, an idiom, jargon, or a metaphor.
- Keep a noun cluster to three words.
- Use a bulleted list for more than three related items. A list of code names in a fixed order can stay in the
  sentence, as `use`, `alias`, `import`, `require`.
- Give the reason before a warning or a caution.
- Copy a code identifier, a file path, or a term of a third party or of the domain exactly. The word limits do not
  apply to it.

## MATCH - Let the pattern carry the logic

A function head and a pattern carry the logic. A branch uses the simplest construct that fits. Data flows from a value
through a pipeline.

### MATCH-1

When a function head cannot hold the branch, pick the first construct that fits. Every clause counts as a path, and a
catch-all clause counts. Do not use `unless`.[^unless]

1. an `if` and an `else`, when the branch tests one value for truth and has one path or two paths
2. a `case`, when the branch matches a pattern, or when it has three paths or more on one piece of data
3. a `cond`, when the branches test different data, or test one value with calls that a guard cannot hold
4. a `with`, when the branch runs steps that can each fail

Give a `with` only clauses that match with `<-`. Compute a value that cannot fail before the `with`, or where the
clause uses it.[^with] Give the pipeline of a clause its own function.[^chain] Keep the `else` short. When a clause
needs its own error, return that error from the function of the clause.[^else]

### MATCH-2

When you write a pipeline, start it with a value, or with a call that builds an empty value, as `MapSet.new/0`. Do
not write a helper that only wraps a result. Pipe the result into a `case` instead.

### MATCH-3

When you write a pipeline, put the value on the first line and each `|>` on a line of its own. Do not write a pipeline
on one line, as `topics |> Enum.map(& &1.name) |> Enum.sort()`.[^pipe]

## NAME - Names say what a value is

A name says what a value is or what it does. It does not say who reads it.

### NAME-1

When you name a function, name the scope before the object, as in `<verb>_<scope>_<object>[_<attribute>]`. Do not use
`_for_`. When you name a map, name its key before its values, as `topic_events`.

## LAYOUT - Each part has one place

A reader finds each part of a module in the same place.

### LAYOUT-1

When a module holds directives, put them in the order `use`, `alias`, `import`, `require`. Sort each group, and put a
blank line between groups. Do not use `:as`.

### LAYOUT-2

When a module defines a private function, count the public functions that call it, directly or through other private
functions. A callback counts as a public function. A function with many clauses counts as one function.

- When one public function calls it, put it under that public function, before the next public function.
- When more than one public function calls it, put it at the end of the module.

Order the private functions under each public function and at the end by the first call, depth first.[^order]

### LAYOUT-3

When a module holds many public functions, group them by the domain object that they act on. Order each group by the
lifecycle of that object.

## TEST - Tests run the code as a caller runs it

A test reaches the code through the public API, and it asserts on what a caller sees.

### TEST-1

When a test runs code that logs, let the log print. Do not capture it with `ExUnit.CaptureLog`.

## Footnotes

[^unless]: Elixir deprecates `unless`, and its docs say to use `if/2` instead.

[^with]: An assignment in a `with` cannot fail, so it hides the clauses that can.

[^chain]: A clause names one step that can fail. The steps that build its value belong under a name of their own.

[^else]: An `else` that tells apart the errors of many clauses hides which step failed. See
[Complex `else` clauses in `with`](https://hexdocs.pm/elixir/code-anti-patterns.html#complex-else-clauses-in-with) in
the Elixir anti-patterns.

[^pipe]: Each step of the pipeline then starts a line. A reader scans the steps from the left, and a diff shows only the
step that changes.

[^order]: A reader meets each private function after the code that calls it. A private function at the end serves many
public functions, so no one public function owns it.
