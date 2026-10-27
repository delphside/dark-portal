# Dark Portal: the rules that are its own

@programme/CLAUDE.md

## Where the programme lives

- The generic rules, skills and agents are in the `programme/` submodule
  (`delphside/programme`), pinned at a commit of this repository's choosing.
  Run `git submodule update --init` after a clone or a pull that moves the pin.
- Dark Portal's records are in the programme repository's live checkout, under
  `applications/dark-portal/`, never in the pinned submodule.

## The session on the web

- The application's own notes issue, where a web session leaves its note for
  the local session when no issue concerns the work, is #11.

## Changes

- No commit stamp yet: it is set with the first release (#11).
