# Plan: Issue #96 - Per-entity recurrence for deadlines/system events with missed-run policy

## Overview
Add per-entity recurrence support to the deadline/scheduling system with missed-run policies (skip/fireOnce/fireAll) and validity windows. This extends the existing ScheduleDeadline (one-shot, per-entity) rather than modifying global cron scheduling.

## Design decisions
- Extend DeadlineSpec with optional recurrence field (backward compatible)
- Add MissedRunPolicy (Skip/FireOnce/FireAll)
- Add RecurrenceSpec with schedule (cron), policy, start_at/end_at/valid_until
- Store recurrence state in deadlines table (extend schema)
- For recurring deadlines: when fired, generate next occurrence(s) based on policy
- FireAll with missed runs: catch-up logic generates separate deadline rows for each missed occurrence within validity window
- Cancellation (CancelDeadline by tags) continues to work

## Implementation steps

### 1. Core types (skilj-core/src/plugin/mod.rs)
- Add MissedRunPolicy enum
- Add RecurrenceSpec struct
- Extend DeadlineSpec<P> to add recurrence: Option<RecurrenceSpec>
- Update any code constructing DeadlineSpec

### 2. DB schema (skilj-core/src/db/mod.rs)
- Extend deadlines table DDL with recurrence columns (recurrence_schedule, recurrence_policy, recurrence_start_at, recurrence_end_at, recurrence_valid_until, recurrence_parent_id, recurrence_sequence, next_fire_at)
- Add migration/patching in ensure_deadlines_table
- Update row structs

### 3. Logic for recurrence
- Extend catch_up_schedule_deadline to handle recurring specs
- Extend fire_due_deadlines to generate next occurrence(s) after firing a recurring deadline
- Handle FireAll policy during catch-up to generate missed occurrences

### 4. Tests
- Add tests for recurring deadlines with each policy
- Validity windows
- Cancellation of recurring deadlines
- Missed occurrences during catch-up

### 5. Docs/specs (if needed)
- Update architecture.md if needed
