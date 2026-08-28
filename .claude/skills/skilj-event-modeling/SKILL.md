---
name: skilj-event-modeling
description: >
  A structured discovery conversation for designing a skilj bounded
  context's event/command shape before any Rust gets written - naming
  candidate events, identifying which commands trigger them and when
  they're accepted or rejected, and finding the DCB consistency tags
  (skilj's own divergence from classic aggregate-ID modeling). Produces
  a reviewable markdown summary, not code. Triggers on "help me design
  the events for X", "what events/commands does my domain need", "model
  this as event storming", or describing a domain process and asking how
  it maps to skilj. Do NOT use once the event/command shape is already
  decided and you're implementing it in Rust - use the `skilj` skill for
  that instead.
---

# skilj event modeling

Walks a domain expert from "here's what happens in my domain" to a
concrete, reviewable list of event types, command types, and their DCB
tag candidates - before committing to a single Rust struct. The output
is a markdown document (see
[references/output-format.md](references/output-format.md)), reviewed
by a human, not code and not a registration call. Once the shape is
settled, the `skilj` skill covers turning it into real
`EventType`/`CommandType`/`Projection` impls.

This is domain-shape discovery only - narrower than a full specification
elicitation. If the conversation grows into needing access-control
rules, timeouts, external integrations, or anything beyond "what events/
commands exist and what tags scope them," that's a different, bigger
job than this skill covers; say so rather than trying to force it into
this shape.

## The three questions, in order

Ask one at a time. Don't move to the next question for an event until
the current one is answered with something concrete - "obviously" or a
vague "yes" isn't an answer (see "Common traps" below).

### 1. What happened? (naming events)

Ask the domain expert to describe what happens in their own words first
- don't impose structure yet. As they talk, extract candidate events in
**past tense**: "MoneyDeposited", never "DepositMoney" - this is the
EventStorming convention, and it matters here specifically because
skilj's own `EventType`/`CommandType` split mirrors it exactly (see the
`skilj` skill's own `references/event-type.md`/`command-type.md` for
the Rust-level shapes these become).

Good prompts: "Walk me through what happens when [a customer places an
order]." "What's the very first thing that happens?" "And then what?"
"Is there anything that happens on a schedule, without anyone asking for
it?" (a candidate for `system_triggered_allowed` later, but don't name
that Rust concept yet - just note it as "happens automatically, on a
schedule").

For each candidate event, capture: its name, and roughly what data it
carries (a sketch, not a schema - "an order has an id, which items, and
a total" is enough for now).

### 2. What triggers it, and when does it not happen? (commands)

For each event, ask: "What has to happen for this to occur? Is it
something someone asks for, or does it just happen?" If someone asks for
it, that's a command - imperative: "DepositMoney"
triggers "MoneyDeposited".

Then, critically, ask what makes it fail: "Under what conditions would
this be rejected instead?" Push for specifics, the same way you would
for any edge case: "insufficient funds" isn't enough - "the withdrawal
amount is more than the current balance" is. Every rejection needs both
a human-readable reason and a short, stable machine label (skilj's own
`CommandDecision::Rejected { reason, kind }` shape - again, don't name
the Rust type, just capture both forms of the answer now).

A command can trigger more than one event, or none (a rejection emits
none). Capture each accept/reject condition against the specific
event(s) it produces.

### 3. What would need to see this, to decide something else? (DCB tags)

This is the one place skilj's own model diverges from textbook
EventStorming, and it's worth getting right - see
[references/dcb-tags.md](references/dcb-tags.md) for the full technique
and a real worked example. The short version: for each event, ask "if a
*different* command were being decided, what field's value would it
need to match on this event to know about it?" That field is a tag
candidate. An event can have more than one tag (see the reference for
why that's often exactly right, not a smell).

Don't ask this before an event and its triggering command are both
named - tags describe a relationship between a command's own decision
and prior events, so there has to be a decision to talk about first.

## Producing the summary

Once a handful of events/commands are named with real accept/reject
conditions and tag candidates, write the structured summary (see
[references/output-format.md](references/output-format.md) for the
exact shape) to a file and show it to the domain expert - not as a final
answer, as something to react to. Expect corrections; that's the point
of writing it down before Rust exists to make changing it expensive.

Leave genuinely undecided things as open questions in the document
rather than guessing - a domain expert correcting a stated assumption is
a much better outcome than one silently agreeing with a wrong one.

## Elicitation principles

Borrowed from this project's own `allium:elicit` skill (a close relative
- full specification elicitation, a bigger job than this one, but the
same underlying discipline):

- **One question at a time.** Not "what events do you have and what
  triggers them and what are the tags" in one breath.
- **Work through implications.** "You said a withdrawal over the
  balance is rejected. What if the balance is exactly zero and someone
  withdraws zero?" Don't accept the first answer as necessarily complete.
- **Record open questions rather than assume.** "I'm not sure whether a
  cancelled order can be reopened - let me note that as an open
  question" beats silently picking an answer.
- **Prioritise depth over breadth.** Fully work out the most important
  event/command pair before moving to the next. A coarse list with open
  questions the expert can return to is a better outcome than everything
  developed shallowly.

### Common traps

- **The "obviously" trap.** "Obviously a manager approves it" - probe
  anyway. Is there ever a case where they don't need to?
- **The "vague agreement" trap.** "Yes, orders can be cancelled" isn't
  an answer. Cancelled by whom? Until when? What happens to anything
  already in progress?
- **The "missing actor" trap.** "The reservation expires" - who or what
  expires it? A scheduled check, or nothing at all until someone looks?
- **The "single aggregate" trap** - specific to this skill, not
  borrowed: resist the urge to force every event onto one "owning"
  entity the way classic aggregate-based modeling would. Ask the DCB
  question (§3 above) honestly for each event, even when an aggregate-ID
  habit suggests there's only one obvious tag.
