# Check two entities in one decision

After the [wallet tutorial](wallet-tutorial.md), read the tested
[courses domain](../skilj-demo/src/courses.rs). An enrollment must obey
two rules: a course cannot exceed its capacity, and a student cannot have
more than three active courses. Both facts belong in the same decision.

## Select both histories

`OpenCourse` emits `CourseOpened` with a `course` tag. `EnrollStudentInCourse`
and `DropCourse`, and the events `StudentEnrolled` and `StudentUnenrolled`,
use this mapping extracted from the domain source:

```rust
fn student_and_course_tags() -> Vec<TagMapping> {
    vec![
        TagMapping::new("student", "student_id"),
        TagMapping::new("course", "course_id"),
    ]
}
```

Enrolling student `alice` in course `rust` selects the **union** of the
events tagged `student: alice` and those tagged `course: rust`, within
the `courses` bounded context. This includes other students in `rust`
and Alice's enrollments in other courses. It is not an intersection
requiring both tags on each event.

| Event | Selected? | Fact used |
| --- | --- | --- |
| Course `rust` opened with capacity 2 | Yes | Course capacity |
| Bob enrolled in `rust` | Yes | Occupied seat |
| Alice enrolled in `databases` | Yes | Alice's course count |
| Bob enrolled in `databases` | No | Neither requested tag matches |

Unlike the wallet fold, each fold must filter the union by the relevant
payload ID. `active_students_in_course` filters by `course_id` and
`active_courses_for_student` filters by `student_id`. Both process events
in order, adding enrollments and removing unenrollments.

## Decide and emit once

`EnrollStudentInCourse::decide` checks, in order:

1. The course exists (`course_not_found` otherwise).
2. This enrollment is new (`already_enrolled` otherwise).
3. The course has a seat (`course_full` otherwise).
4. The student has a slot (`student_course_limit_reached` otherwise).

An accepted decision emits one `StudentEnrolled` event carrying both tags.
Later decisions for either the course or the student can find it.
`DropCourse` emits `StudentUnenrolled`, releasing both the seat and the
student's slot. `CourseRoster` and `StudentSchedule` project the two views.

The boundary includes both entities inside one bounded context and one
command transaction. SkilJ checks for changes to the selected history
before committing; a stale view cannot silently admit a second enrollment
for the last seat. This example does not provide a transaction across
bounded contexts or external services. Use [Temporal](temporal-integration.md)
when coordinating a process with external effects.

## Run the database-backed example

From the repository root:

```sh
cargo test -p skilj-demo --test courses -- --nocapture
```

The [test suite](../skilj-demo/tests/courses.rs) covers capacity, unopened
courses, duplicate enrollment, the student limit, dropping a course and
reopening an existing course. Its
`concurrent_enrollments_for_the_last_seat_are_serialized_correctly` test
races two HTTP requests for one seat and checks that exactly one succeeds.
These are in-process REST requests backed by real Postgres.

The harness uses `DATABASE_URL` or embedded Postgres and can print
`skipping:` when provisioning fails. Check the output: a passing test
that skipped provides no database coverage. Use a disposable server with
`CREATEDB`; the harness recreates its named test database. In CI,
`scripts/ci-test.sh` rejects any skip. See
[environment troubleshooting](troubleshooting.md) if setup fails.

Pure fixtures are useful for extending the decision rules, but supplying a
union by hand cannot verify that the engine selected it or detected a
concurrent change. Retain database-backed checks when changing tag mappings.
The selected last-seat race is evidence for that scenario, not exhaustive
exploration of all possible interleavings.
