# Finding DCB tags

skilj uses Dynamic Consistency Boundaries (DCB), not classic
aggregate-per-stream modeling. This is the one place a generic
EventStorming background can mislead you, so it's worth understanding
the real mechanism, not just pattern-matching on "this looks like an
aggregate ID."

## The question to ask

For each event, and separately for each command: **"If a different
command's own decision needed to know about this, what field's value
would it match on?"**

That field becomes a tag. A command's `decide()` then sees exactly the
prior events sharing at least one of its own tags - never the whole
event history, and never artificially scoped to one "owning" entity
either.

## Worked example: the simple, single-tag case

A bank account. `MoneyDeposited`/`MoneyWithdrawn` both carry an
`account_id`. `WithdrawMoney`'s own decision ("is there enough balance")
needs to see every prior deposit/withdrawal for *that account* -
nothing from any other account. One tag, `account`, mapped from
`account_id` on both the events and the command. This is the case that
looks like a classic aggregate ID, and here it basically is one - a
single shared field, one command family, one clean boundary.

## Worked example: the case a single aggregate ID can't express

A course enrollment system, with two rules that have to hold
simultaneously:

- a course never accepts more enrollments than its own capacity;
- a student is never enrolled in more than a handful of courses at once.

Those two facts live on what classic aggregate modeling would call two
separate aggregates - `Course` and `Student`. No single aggregate's own
transaction can see both facts at once, which is exactly the situation
that normally forces a saga or process manager: reserve a seat on the
course, separately check the student's own count, compensate if either
step fails partway through. Two writes, a window where the two facts
disagree, and compensation logic to get right.

The DCB answer is simpler, and it's the reason to ask the tag question
honestly instead of reaching for a single ID out of habit:
`EnrollStudentInCourse` tags on **both** `student` (from `student_id`)
and `course` (from `course_id`). Its `decide()` then receives the
*union* of that one student's own enrollment history and that one
course's own enrollment history, in a single synchronous call - both
invariants checked, the accepted event emitted, atomically. No saga, no
reservation step, no compensation, because there was never a moment
where only one of the two facts was visible.

(This is a real, tested example - `skilj-demo/src/courses.rs`'s own
module doc comment explains it in exactly these terms, and
`skilj-demo/tests/courses.rs` has a test that races two enrollments for
a course's last seat to prove the atomicity holds under real
concurrency, not just in a single-threaded reading of the code.)

## What this means for the modeling conversation

- **An event or command can have more than one tag.** Don't stop at the
  first obvious one - ask the question again for each fact the
  triggering command's own decision depends on. "What field, if it
  matched, would matter to some other decision?"
- **Don't force everything under one "owning" entity.** If two
  different facts genuinely need to be checked together for one
  decision, that's exactly the multi-tag shape above, not a sign the
  modeling is wrong.
- **A tag's value has to come from somewhere in the payload.** Every tag
  candidate should trace to a specific field you already named while
  sketching the event/command's own data in step 1/2 of the main
  workflow - if it doesn't exist yet, that's a gap in the payload sketch
  to fix first, not a tag to note anyway.
- **Not every field is a tag.** Tagging is deliberately opt-in and
  narrow - only fields another command's own decision genuinely needs to
  match on. A field nothing else ever needs to see by isn't a tag
  candidate, it's just payload data.
