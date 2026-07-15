# Colored keywords in Python comments

`# NOTE: ...` is a plain Python comment — the color does not live in the file.
Your editor (VS Code natively does very little here; usually it is an extension like
**Better Comments**, **TODO Highlight**, **Todo Tree** or **Comment Anchors**)
recognises keywords at the start of a comment and highlights them differently from the
rest of the text. It is purely cosmetic syntax highlighting — in the notebook's JSON
the comment is just text in the `source` field, zero magic.

If you want to use it deliberately as a convention (e.g. `# NOTE:` for important
caveats, `# TODO:` for exercises to do), go ahead — it works anywhere your editor
renders comments, and degrades gracefully to plain text everywhere else.

## Keywords that get colored

Which words light up (and in what color) depends on the extension + theme, so treat
this as a menu, not a guarantee:

### Conventional "codetags" — recognised by most tools

| tag | conventional meaning |
| --- | --- |
| `TODO` | work to do |
| `FIXME` | broken, needs fixing |
| `BUG` | known defect |
| `HACK` | ugly but deliberate workaround |
| `XXX` | danger / big warning, needs attention |
| `NOTE` | important remark for the reader |
| `WARNING` | caution for whoever touches this |
| `OPTIMIZE` / `PERF` | performance improvement opportunity |
| `REVIEW` | needs a second pair of eyes |
| `DEPRECATED` | scheduled for removal |
| `TBD` | to be decided |
| `TEMP` / `KLUDGE` | temporary code, remove later |
| `IDEA` / `QUESTION` | open thought / open question |
| `SECURITY` | security-relevant spot |
| `REFACTOR` | works, but restructure it |
| `DEBUG` | debug-only leftovers |

(The classic list comes from PEP 350 "codetags"; most extensions support a subset by
default and let you add the rest in settings.)

### Per-extension defaults

- **Better Comments** (symbol-based, colors out of the box):
  - `# !` — red (alert)
  - `# ?` — blue (question)
  - `# *` — green (highlight)
  - `# TODO` — orange
  - `# //` — grey strikethrough ("commented-out")
- **TODO Highlight**: `TODO:`, `FIXME:` by default; anything else via
  `todohighlight.keywords`.
- **Todo Tree**: `TODO`, `FIXME`, `BUG`, `HACK`, `XXX`, `[ ]`, `[x]` by default
  (also builds a tree view of all tags across the repo — handy for exercise TODOs).
- **JetBrains / PyCharm** (natively): `TODO`, `FIXME`.

## Adding your own keywords (VS Code, Better Comments)

`settings.json`:

```json
"better-comments.tags": [
  { "tag": "NOTE:",    "color": "#3498DB" },
  { "tag": "WARNING:", "color": "#FF8C00" },
  { "tag": "EXAM:",    "color": "#98C379" }
]
```

Custom tags like `# EXAM:` (things that show up on certification exams) can be a nice
repo-specific convention — the tag list is fully yours.
