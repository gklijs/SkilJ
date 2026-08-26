//! The "hard" half of this demo (see the module doc comment in `lib.rs`).
//! `EnrollStudentInCourse` has to hold two invariants at once:
//!
//! - a course never gets more active enrollments than its own `capacity`;
//! - a student is never actively enrolled in more than
//!   [`MAX_ACTIVE_COURSES_PER_STUDENT`] courses at once.
//!
//! Those facts live on what a classic one-stream-per-aggregate event
//! store would model as two separate aggregates (`Course`, `Student`),
//! which is exactly the case that needs a saga/process manager there: no
//! single aggregate's own transaction can see both facts at once, so
//! you'd reserve a seat on the course aggregate, then check the student
//! aggregate, then compensate if either step fails partway - two writes,
//! an in-between state, and compensation logic to get right.
//!
//! Here `EnrollStudentInCourse::tag_mappings()` just names both `student`
//! and `course` (`derive_tags` - docs/architecture.md §1.7). `decide()`
//! then receives the *union* of that one student's history and that one
//! course's history as `matching_events`, in one synchronous call - both
//! invariants checked, and the accepted event emitted, atomically, with
//! no saga, no reservation, no compensation. `skilj-core::db::
//! submit_command`'s own commit lock (docs/architecture.md §2.2.2) is
//! what keeps this atomic under real concurrency too, not just logically
//! consistent in a single-threaded read - `skilj-demo/tests/courses.rs`
//! has a test that races two enrollments for the last seat in a course to
//! prove it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{auto_register, CommandType, EventType, Projection};
use skilj_core::event_store::Event;
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{CommandDecision, EventSpec, TagMapping};

pub const BOUNDED_CONTEXT: &str = "courses";

/// A fixed policy constant, not itself event-sourced - kept simple and
/// visible on purpose, since the point of this demo is the two-tag
/// `decide()` below, not configurable policy storage.
pub const MAX_ACTIVE_COURSES_PER_STUDENT: usize = 3;

fn course_tag() -> Vec<TagMapping> {
    vec![TagMapping {
        key: "course".into(),
        field: "course_id".into(),
    }]
}

fn student_and_course_tags() -> Vec<TagMapping> {
    vec![
        TagMapping {
            key: "student".into(),
            field: "student_id".into(),
        },
        TagMapping {
            key: "course".into(),
            field: "course_id".into(),
        },
    ]
}

// --- events ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CourseOpenedPayload {
    pub course_id: String,
    pub capacity: i64,
}

pub struct CourseOpened;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for CourseOpened {
    type Payload = CourseOpenedPayload;
    const NAME: &'static str = "CourseOpened";
    fn tag_mappings() -> Vec<TagMapping> {
        course_tag()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StudentEnrolledPayload {
    pub student_id: String,
    pub course_id: String,
}

pub struct StudentEnrolled;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for StudentEnrolled {
    type Payload = StudentEnrolledPayload;
    const NAME: &'static str = "StudentEnrolled";
    /// Tagged on *both* - this is what lets a later `EnrollStudentInCourse`
    /// (for either this student or this course) find this event via
    /// `matching_events`, regardless of which of the two tags it matched
    /// on.
    fn tag_mappings() -> Vec<TagMapping> {
        student_and_course_tags()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StudentUnenrolledPayload {
    pub student_id: String,
    pub course_id: String,
}

pub struct StudentUnenrolled;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for StudentUnenrolled {
    type Payload = StudentUnenrolledPayload;
    const NAME: &'static str = "StudentUnenrolled";
    fn tag_mappings() -> Vec<TagMapping> {
        student_and_course_tags()
    }
}

/// This bounded context's own hand-written event enum - see
/// docs/architecture.md §1.4/§1.6.
pub enum CoursesEvent {
    CourseOpened(CourseOpenedPayload),
    StudentEnrolled(StudentEnrolledPayload),
    StudentUnenrolled(StudentUnenrolledPayload),
}

impl BoundedContextEvent for CoursesEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "CourseOpened" => {
                Some(serde_json::from_str(&event.payload).map(CoursesEvent::CourseOpened))
            }
            "StudentEnrolled" => {
                Some(serde_json::from_str(&event.payload).map(CoursesEvent::StudentEnrolled))
            }
            "StudentUnenrolled" => {
                Some(serde_json::from_str(&event.payload).map(CoursesEvent::StudentUnenrolled))
            }
            _ => None,
        }
    }
}

/// `matching_events` for `EnrollStudentInCourse{student_id, course_id}`
/// is the *union* of this student's own events and this course's own
/// events - an event belonging to some other student in this same course
/// is included too (it matched the `course` tag), so every fold below
/// filters by the actual payload field, not just by membership in the
/// slice.
fn course_capacity(matching_events: &[CoursesEvent], course_id: &str) -> Option<i64> {
    matching_events.iter().find_map(|event| match event {
        CoursesEvent::CourseOpened(p) if p.course_id == course_id => Some(p.capacity),
        _ => None,
    })
}

/// Currently-enrolled student ids for `course_id`, folded in the order
/// `matching_events` arrived in (oldest first - see `db::list_events*`),
/// so an unenroll correctly cancels an earlier enroll.
fn active_students_in_course<'a>(
    matching_events: &'a [CoursesEvent],
    course_id: &str,
) -> Vec<&'a str> {
    let mut active: Vec<&str> = Vec::new();
    for event in matching_events {
        match event {
            CoursesEvent::StudentEnrolled(p) if p.course_id == course_id => {
                active.push(p.student_id.as_str());
            }
            CoursesEvent::StudentUnenrolled(p) if p.course_id == course_id => {
                active.retain(|s| *s != p.student_id);
            }
            _ => {}
        }
    }
    active
}

/// Currently-active course ids for `student_id` - the mirror of
/// `active_students_in_course`, folded the same way.
fn active_courses_for_student<'a>(
    matching_events: &'a [CoursesEvent],
    student_id: &str,
) -> Vec<&'a str> {
    let mut active: Vec<&str> = Vec::new();
    for event in matching_events {
        match event {
            CoursesEvent::StudentEnrolled(p) if p.student_id == student_id => {
                active.push(p.course_id.as_str());
            }
            CoursesEvent::StudentUnenrolled(p) if p.student_id == student_id => {
                active.retain(|c| *c != p.course_id);
            }
            _ => {}
        }
    }
    active
}

// --- commands ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OpenCoursePayload {
    pub course_id: String,
    pub capacity: i64,
}

pub struct OpenCourse;

#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for OpenCourse {
    type Payload = OpenCoursePayload;
    type Event = CoursesEvent;
    const NAME: &'static str = "OpenCourse";
    fn tag_mappings() -> Vec<TagMapping> {
        course_tag()
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        if payload.capacity <= 0 {
            return CommandDecision::Rejected {
                reason: "capacity must be positive".into(),
                kind: "invalid_capacity".into(),
            };
        }
        if course_capacity(matching_events, &payload.course_id).is_some() {
            return CommandDecision::Rejected {
                reason: format!("course {} is already open", payload.course_id),
                kind: "course_already_open".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "CourseOpened".into(),
                payload: serde_json::json!({
                    "course_id": payload.course_id,
                    "capacity": payload.capacity,
                }),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EnrollStudentInCoursePayload {
    pub student_id: String,
    pub course_id: String,
}

pub struct EnrollStudentInCourse;

#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for EnrollStudentInCourse {
    type Payload = EnrollStudentInCoursePayload;
    type Event = CoursesEvent;
    const NAME: &'static str = "EnrollStudentInCourse";
    /// The whole point of this demo - see the module doc comment above.
    fn tag_mappings() -> Vec<TagMapping> {
        student_and_course_tags()
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        let Some(capacity) = course_capacity(matching_events, &payload.course_id) else {
            return CommandDecision::Rejected {
                reason: format!("course {} has not been opened", payload.course_id),
                kind: "course_not_found".into(),
            };
        };

        let course_roster = active_students_in_course(matching_events, &payload.course_id);
        if course_roster.contains(&payload.student_id.as_str()) {
            return CommandDecision::Rejected {
                reason: format!(
                    "student {} is already enrolled in course {}",
                    payload.student_id, payload.course_id
                ),
                kind: "already_enrolled".into(),
            };
        }
        // Invariant 1: the course's own capacity.
        if course_roster.len() as i64 >= capacity {
            return CommandDecision::Rejected {
                reason: format!("course {} is full", payload.course_id),
                kind: "course_full".into(),
            };
        }

        // Invariant 2: the student's own course limit - checked in the
        // same call, against the same `matching_events`, because
        // `tag_mappings()` above asked for both tags up front. No second
        // round trip, no separate aggregate transaction.
        let student_schedule = active_courses_for_student(matching_events, &payload.student_id);
        if student_schedule.len() >= MAX_ACTIVE_COURSES_PER_STUDENT {
            return CommandDecision::Rejected {
                reason: format!(
                    "student {} is already enrolled in {} courses (limit {MAX_ACTIVE_COURSES_PER_STUDENT})",
                    payload.student_id,
                    student_schedule.len()
                ),
                kind: "student_course_limit_reached".into(),
            };
        }

        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StudentEnrolled".into(),
                payload: serde_json::json!({
                    "student_id": payload.student_id,
                    "course_id": payload.course_id,
                }),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DropCoursePayload {
    pub student_id: String,
    pub course_id: String,
}

pub struct DropCourse;

#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for DropCourse {
    type Payload = DropCoursePayload;
    type Event = CoursesEvent;
    const NAME: &'static str = "DropCourse";
    fn tag_mappings() -> Vec<TagMapping> {
        student_and_course_tags()
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        let course_roster = active_students_in_course(matching_events, &payload.course_id);
        if !course_roster.contains(&payload.student_id.as_str()) {
            return CommandDecision::Rejected {
                reason: format!(
                    "student {} is not enrolled in course {}",
                    payload.student_id, payload.course_id
                ),
                kind: "not_enrolled".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StudentUnenrolled".into(),
                payload: serde_json::json!({
                    "student_id": payload.student_id,
                    "course_id": payload.course_id,
                }),
            }],
        }
    }
}

// --- projections ---

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct CourseRosterState {
    pub capacity: Option<i64>,
    pub enrolled_student_ids: Vec<String>,
}

/// Keyed by `course_id`.
pub struct CourseRoster;

#[auto_register(BOUNDED_CONTEXT)]
impl Projection for CourseRoster {
    type State = CourseRosterState;
    type Event = CoursesEvent;
    const NAME: &'static str = "CourseRoster";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["CourseOpened", "StudentEnrolled", "StudentUnenrolled"]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            CoursesEvent::CourseOpened(p) => vec![p.course_id.clone()],
            CoursesEvent::StudentEnrolled(p) => vec![p.course_id.clone()],
            CoursesEvent::StudentUnenrolled(p) => vec![p.course_id.clone()],
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            CoursesEvent::CourseOpened(p) => state.capacity = Some(p.capacity),
            CoursesEvent::StudentEnrolled(p) => {
                state.enrolled_student_ids.push(p.student_id.clone())
            }
            CoursesEvent::StudentUnenrolled(p) => {
                state.enrolled_student_ids.retain(|s| s != &p.student_id)
            }
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct StudentScheduleState {
    pub course_ids: Vec<String>,
}

/// Keyed by `student_id`.
pub struct StudentSchedule;

#[auto_register(BOUNDED_CONTEXT)]
impl Projection for StudentSchedule {
    type State = StudentScheduleState;
    type Event = CoursesEvent;
    const NAME: &'static str = "StudentSchedule";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["StudentEnrolled", "StudentUnenrolled"]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            CoursesEvent::CourseOpened(_) => vec![],
            CoursesEvent::StudentEnrolled(p) => vec![p.student_id.clone()],
            CoursesEvent::StudentUnenrolled(p) => vec![p.student_id.clone()],
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            CoursesEvent::CourseOpened(_) => {}
            CoursesEvent::StudentEnrolled(p) => state.course_ids.push(p.course_id.clone()),
            CoursesEvent::StudentUnenrolled(p) => state.course_ids.retain(|c| c != &p.course_id),
        }
    }
}
