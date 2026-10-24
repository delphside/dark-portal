# Dark Portal: the rules that are its own

@programme/CLAUDE.md

## Where the programme lives

- The generic rules, skills and agents are in the `programme/` submodule
  (`delphside/programme`), pinned at a commit of this repository's choosing.
  Run `git submodule update --init` after a clone or a pull that moves the pin.
- Dark Portal's records are in the programme repository's live checkout, under
  `applications/dark-portal/`, never in the pinned submodule.

## Changes

- Commits and pull requests say `Refs #N`, never `Closes #N`: this repository
  does not close issues from merged pull requests (owner, 2026-10-24), so an
  issue is closed by hand after its post-deployment check.
- No commit stamp yet: it is set with the first release (#11).
