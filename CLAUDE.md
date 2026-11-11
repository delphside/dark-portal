# Dark Portal: the rules that are its own

@programme/CLAUDE.md

## Where the programme lives

- The generic rules, skills and agents are in the `programme/` submodule
  (`delphside/programme`), pinned at a commit of this repository's choosing.
  `.claude/skills` and `.claude/agents` link into it. Run
  `git submodule update --init` after a clone or a pull that moves the pin.
- **The `docs/1.x` paths in the imported rules are read in `programme/`.**
- **Records are in the live checkout.**
  - `records/` and `applications/` paths are read and changed in the programme
    repository's live checkout, never in the pinned submodule.
  - The checkout is beside this repository's main worktree at `../programme`
    or wherever `PROGRAMME_DIR` says. Changes go on its `main`; a cloud session
    makes them by a short-lived pull request.
  - Dark Portal's records are under `applications/dark-portal/`.

## The session on the web

- The application's own notes issue, where a web session leaves its note for
  the local session when no issue concerns the work, is #11.

## Changes

- Until Claude sets a commit stamp in the first release's work package (#15)
  and replaces this line, commit subjects carry none: this sets aside the
  programme's stamp rule.
