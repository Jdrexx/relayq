---
name: procoder
description: >-
  Work like a senior developer in a repository governed by procoder: run the
  commit gate before calling anything done, format and lint through the
  binary, and drive the spec, plan, todo, backlog, and sprint chain in
  .procoder/. Use this skill when the repository contains a .procoder/
  directory or an AGENTS.md naming procoder, or when the user asks to run the
  gate, check formatting, open a spec or plan, close a task, or prepare a
  release.
license: Apache-2.0
metadata:
  category: development
  author: pascal-watteel
---

# Agent guide

This repository is governed by [Procoder](https://github.com/azrtydxb/procoder).
The `procoder` binary computes; you act. It never edits a file behind your
back, and a file it could not check is never reported as clean.

Every agent that reads `AGENTS.md` picks this up automatically. Run
`procoder agents` to emit the equivalent rule file for a host that wants
its own format (Cursor, Windsurf, Cline, Copilot, Codex, and the rest).

## The contract

- Before calling any work finished, run `procoder check` — the commit
  gate. Blocking findings (unformatted files, conflict markers, junk
  files, secrets, AI-attribution lines) get fixed, not argued with.
- `procoder format <file>` prints the formatted result; you review it and
  write it. The binary never touches the file itself.
- Run `procoder test` before claiming anything works. A suite that was
  not run is never the same as a green one — say which it was.
- Never add AI-attribution lines (`Co-Authored-By`, "generated with") to
  commits or PRs. `procoder scrub` verifies. If the gate blocks one you
  did not write, the host appended it — turn it off at the source rather
  than amending every commit.
- A deliberate corner-cut carries a `debt:` comment naming both the
  ceiling and the condition to revisit it. `procoder debt` harvests those
  into a ledger and flags any marker with no revisit trigger.

## Before you write code

Climb this ladder and stop at the first rung that holds:

1. Does this need to exist at all?
2. Does this repository already have it? (`procoder index find <symbol>`)
3. Does the standard library do it?
4. Does an already-installed dependency do it?
5. Only then: the minimum code that works.

The ladder runs _after_ you understand the problem, never instead of it.
A small diff in the wrong place is not lazy — it is a second bug. For a
bug fix, find every caller of what you are about to touch: one guard
where the paths converge beats a patch in the one path the ticket named.

## The work chain

Non-trivial work starts above the code. Each link refuses to advance
until its own gap is closed.

| Command            | Lives in             | Refuses while                                   |
| ------------------ | -------------------- | ----------------------------------------------- |
| `procoder spec`    | `.procoder/specs/`   | a section is empty or a criterion is untestable |
| `procoder plan`    | `.procoder/plans/`   | a task has no files or steps                    |
| `procoder backlog` | `.procoder/backlog/` | a child story is still open                     |
| `procoder todo`    | `.procoder/todo/`    | acceptance criteria lack evidence               |
| `procoder adr`     | `.procoder/adr/`     | a record is hollow or supersedes nothing        |

Do not game the checkboxes — the controllers ask for evidence.

## Questions are not yours to answer

An undecided spec question, a documentation gap that may be deliberate, a
flagged string that may be a test credential: these are requests for
judgement, not defects. Stop and ask the human in your own words. An
invented answer is indistinguishable from a decision once it is written
down, and the human never learns they were never asked. Record what they
say with `procoder ask --file <path>`.

## Useful entry points

- `procoder status` — branch, dirty files, open tasks, unlearned lessons
- `procoder audit` — every domain's checks over the whole tree
- `procoder security` — secrets on changed files; `--deep` adds SAST and CVEs
- `procoder review` — the review lenses to judge the change against
- `procoder run` — how this project is actually launched
