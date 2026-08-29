//! `surface CommandTypeAdminOperations` - `createCommandToken`. Same
//! shape as `event_type_admin_operations`, scoped to a `CommandType`
//! instead of an `EventType`.

use super::create_type_token_field;
use async_graphql::dynamic::Field;

pub fn create_command_token_field() -> Field {
    create_type_token_field!(
        "createCommandToken",
        "CommandToken",
        "commandTypeName",
        "CommandType",
        skilj_core::db::get_command_type,
        skilj_core::access_control::create_command_token,
        skilj_core::db::insert_command_token
    )
}
