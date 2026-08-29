//! `surface EventTypeAdminOperations` - `createExternalEventToken`,
//! `createDirectCreationToken`, `createEventReadToken`. `AdminAccess`-
//! gated on the target `EventType`'s own bounded context.

use super::create_type_token_field;
use async_graphql::dynamic::Field;

pub fn create_external_event_token_field() -> Field {
    create_type_token_field!(
        "createExternalEventToken",
        "ExternalEventToken",
        "eventTypeName",
        "EventType",
        skilj_core::db::get_event_type,
        skilj_core::access_control::create_external_event_token,
        skilj_core::db::insert_external_event_token
    )
}
pub fn create_direct_creation_token_field() -> Field {
    create_type_token_field!(
        "createDirectCreationToken",
        "DirectCreationToken",
        "eventTypeName",
        "EventType",
        skilj_core::db::get_event_type,
        skilj_core::access_control::create_direct_creation_token,
        skilj_core::db::insert_direct_creation_token
    )
}
pub fn create_event_read_token_field() -> Field {
    create_type_token_field!(
        "createEventReadToken",
        "EventReadToken",
        "eventTypeName",
        "EventType",
        skilj_core::db::get_event_type,
        skilj_core::access_control::create_event_read_token,
        skilj_core::db::insert_event_read_token
    )
}
