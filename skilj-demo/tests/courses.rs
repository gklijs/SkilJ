//! Integration tests for `skilj_demo::courses` - the bounded context that
//! actually demonstrates what the dynamic consistency boundary buys over
//! per-aggregate event sourcing (see its own module doc comment).
//! Everything up to the last test proves the two-invariant `decide()` is
//! logically correct; the last test proves it stays correct under real
//! concurrent HTTP requests, not just when called one at a time.

mod support;

use skilj_demo::courses::{CourseRosterState, StudentScheduleState, BOUNDED_CONTEXT};
use support::{
    accepted, mapping_for, mint_command_token, projection_state, rejection_kind, runtime, setup,
    test_db, trigger, unique_name,
};

#[test]
fn enrolling_past_a_courses_capacity_is_rejected() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let course = unique_name("course");
        let (s1, s2, s3) = (unique_name("student"), unique_name("student"), unique_name("student"));

        let open = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "OpenCourse").await;
        let enroll = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "EnrollStudentInCourse").await;

        let response = trigger(&router, &open, serde_json::json!({ "course_id": course, "capacity": 2 })).await;
        assert!(accepted(&response));

        for student in [&s1, &s2] {
            let response = trigger(
                &router,
                &enroll,
                serde_json::json!({ "student_id": student, "course_id": course }),
            )
            .await;
            assert!(accepted(&response), "{student} should get one of the 2 seats: {response:?}");
        }

        let response = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": s3, "course_id": course }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "course_full");

        let roster: CourseRosterState =
            projection_state(&pool, BOUNDED_CONTEXT, "CourseRoster", &course).await;
        assert_eq!(roster.capacity, Some(2));
        assert_eq!(roster.enrolled_student_ids.len(), 2);
        assert!(!roster.enrolled_student_ids.contains(&s3));
    });
}

#[test]
fn enrolling_in_an_unopened_course_is_rejected() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let course = unique_name("course");
        let student = unique_name("student");

        let enroll = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "EnrollStudentInCourse").await;
        let response = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": student, "course_id": course }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "course_not_found");
    });
}

#[test]
fn enrolling_the_same_student_twice_in_one_course_is_rejected() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let course = unique_name("course");
        let student = unique_name("student");

        let open = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "OpenCourse").await;
        let enroll = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "EnrollStudentInCourse").await;

        trigger(&router, &open, serde_json::json!({ "course_id": course, "capacity": 5 })).await;
        let first = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": student, "course_id": course }),
        )
        .await;
        assert!(accepted(&first));

        let second = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": student, "course_id": course }),
        )
        .await;
        assert!(!accepted(&second));
        assert_eq!(rejection_kind(&second), "already_enrolled");
    });
}

/// The other half of the two-invariant check `EnrollStudentInCourse`
/// makes in one `decide()` call: a student's own limit across courses,
/// checked from the same `matching_events` the course-capacity check
/// above used - see `skilj_demo::courses`' own module doc comment.
#[test]
fn a_student_cannot_exceed_their_own_active_course_limit() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let student = unique_name("student");

        let open = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "OpenCourse").await;
        let enroll = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "EnrollStudentInCourse").await;

        let limit = skilj_demo::courses::MAX_ACTIVE_COURSES_PER_STUDENT;
        let courses: Vec<String> = (0..=limit).map(|_| unique_name("course")).collect();
        for course in &courses {
            let response = trigger(
                &router,
                &open,
                serde_json::json!({ "course_id": course, "capacity": 10 }),
            )
            .await;
            assert!(accepted(&response));
        }

        for course in &courses[..limit] {
            let response = trigger(
                &router,
                &enroll,
                serde_json::json!({ "student_id": student, "course_id": course }),
            )
            .await;
            assert!(accepted(&response), "enrollment {course} within the limit should succeed: {response:?}");
        }

        let one_too_many = &courses[limit];
        let response = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": student, "course_id": one_too_many }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "student_course_limit_reached");

        let schedule: StudentScheduleState =
            projection_state(&pool, BOUNDED_CONTEXT, "StudentSchedule", &student).await;
        assert_eq!(schedule.course_ids.len(), limit);
    });
}

#[test]
fn dropping_a_course_frees_both_the_seat_and_the_students_own_slot() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let course = unique_name("course");
        let (s1, s2) = (unique_name("student"), unique_name("student"));

        let open = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "OpenCourse").await;
        let enroll = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "EnrollStudentInCourse").await;
        let drop = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "DropCourse").await;

        trigger(&router, &open, serde_json::json!({ "course_id": course, "capacity": 1 })).await;
        let response = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": s1, "course_id": course }),
        )
        .await;
        assert!(accepted(&response));

        // The one seat is taken - s2 is rejected until s1 drops.
        let response = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": s2, "course_id": course }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "course_full");

        let response = trigger(
            &router,
            &drop,
            serde_json::json!({ "student_id": s1, "course_id": course }),
        )
        .await;
        assert!(accepted(&response));

        let response = trigger(
            &router,
            &enroll,
            serde_json::json!({ "student_id": s2, "course_id": course }),
        )
        .await;
        assert!(accepted(&response), "the freed seat should now be available: {response:?}");

        let roster: CourseRosterState =
            projection_state(&pool, BOUNDED_CONTEXT, "CourseRoster", &course).await;
        assert_eq!(roster.enrolled_student_ids, vec![s2]);
    });
}

/// The strongest version of this bounded context's own claim (see its
/// module doc comment): two enrollments for the *last* seat, fired at
/// the same time as two real, concurrently in-flight HTTP requests, are
/// still serialized correctly - exactly one wins. This isn't logical
/// correctness under a single caller; it's `skilj-core::db::
/// submit_command`'s optimistic-then-locked retry (docs/architecture.md
/// §2.2.2) actually holding under contention, which a saga/process-
/// manager approach across two separate aggregates would have to
/// re-derive on its own.
#[test]
fn concurrent_enrollments_for_the_last_seat_are_serialized_correctly() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let course = unique_name("course");
        let (student_a, student_b) = (unique_name("student"), unique_name("student"));

        let open = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "OpenCourse").await;
        let enroll = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "EnrollStudentInCourse").await;

        let response = trigger(&router, &open, serde_json::json!({ "course_id": course, "capacity": 1 })).await;
        assert!(accepted(&response));

        let task_a = {
            let router = router.clone();
            let enroll = enroll.clone();
            let course = course.clone();
            let student = student_a.clone();
            tokio::spawn(async move {
                trigger(&router, &enroll, serde_json::json!({ "student_id": student, "course_id": course })).await
            })
        };
        let task_b = {
            let router = router.clone();
            let enroll = enroll.clone();
            let course = course.clone();
            let student = student_b.clone();
            tokio::spawn(async move {
                trigger(&router, &enroll, serde_json::json!({ "student_id": student, "course_id": course })).await
            })
        };

        let (response_a, response_b) = tokio::join!(task_a, task_b);
        let response_a = response_a.expect("task A must not panic");
        let response_b = response_b.expect("task B must not panic");

        let accepted_count = [&response_a, &response_b]
            .into_iter()
            .filter(|r| accepted(r))
            .count();
        assert_eq!(
            accepted_count, 1,
            "exactly one of the two racing enrollments should win the last seat: \
             a={response_a:?} b={response_b:?}"
        );
        let loser = if accepted(&response_a) { &response_b } else { &response_a };
        assert_eq!(rejection_kind(loser), "course_full");

        let roster: CourseRosterState =
            projection_state(&pool, BOUNDED_CONTEXT, "CourseRoster", &course).await;
        assert_eq!(
            roster.enrolled_student_ids.len(),
            1,
            "the roster must reflect exactly one winner, never both or neither"
        );
    });
}
