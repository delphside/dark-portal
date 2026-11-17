<!-- markdownlint-disable-file MD041 -->
<!--
The rules for the body are in *Changes* in
[the programme's `CLAUDE.md`](https://github.com/delphside/programme/blob/main/CLAUDE.md#changes).
Who reviews, and when Claude merges, is
[1.2 §2.2](https://github.com/delphside/programme/blob/main/docs/1.2-change-lifecycle.md#22-when-to-branch-and-what-pre-approved-means).

*Review rounds* is the review record, in the form
[1.1](https://github.com/delphside/programme/blob/main/docs/1.1-programme-tooling.md#claudes-tooling)
gives. Keep `None owed:` where 1.1's *what owes none* row covers the change;
otherwise replace it with the record. `board-digest.py` reads the section's
opening words, so `None owed:` or the record opens it.
-->

Refs #N

<!--
The `Refs #N` line:

- for an issue, replace `N` with the issue this delivers;
- for business-as-usual (BAU) work, an activity in
  [1.4's register](https://github.com/delphside/programme/blob/main/docs/1.4-programme-activities.md#the-register),
  replace the line with `Activity: <id>`, and add `Initiative: <id>` on a line
  of its own where there is one.

Never write `Closes #N`.
-->

## What is being requested

<!--
Which part of the scope this delivers, named against its issue or activity.
-->

## What is deliberately left out

<!--
Scope this pull request does not carry, and why. Where nothing is left out,
write *Nothing*.
-->

## Review

<!--
- Owed: list what the owner is to check, add him as reviewer when opening the
  pull request and ask for his review in the chat.
- Not owed: open this section with `No review owed:` and the reason;
  `board-pr-state.py` then treats it as owing no review.
-->

## Review rounds

None owed:
