pub const BOUNDED_CONTEXT: &str = "customers";
pub struct CustomerEmail;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::BlindTag for CustomerEmail {
    const TAG_KEY: &'static str = "email";
    const NORMALIZER_VERSION: u32 = 1;
    const ERASURE: ::skilj::BlindErasure = ::skilj::BlindErasure::with_subject(
        "customer",
    );
    fn normalize(value: &str) -> String {
        ::skilj::normalize::email(value)
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct EmailClaimedPayload {
    pub customer_id: String,
    pub email: String,
}
pub struct EmailClaimed;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::EventType for EmailClaimed {
    type Payload = EmailClaimedPayload;
    const NAME: &'static str = "EmailClaimed";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        Vec::from([
            ::skilj_core::shared::TagMapping::new("customer", "customer_id"),
            ::skilj_core::shared::TagMapping::blind::<CustomerEmail>("email"),
        ])
    }
    fn sensitive_fields() -> Vec<::skilj_core::shared::SensitiveField> {
        Vec::from([
            ::skilj_core::shared::SensitiveField::new("email", "customer", "customer_id"),
        ])
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct EmailReleasedPayload {
    pub customer_id: String,
    pub email: String,
}
pub struct EmailReleased;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::EventType for EmailReleased {
    type Payload = EmailReleasedPayload;
    const NAME: &'static str = "EmailReleased";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        Vec::from([
            ::skilj_core::shared::TagMapping::new("customer", "customer_id"),
            ::skilj_core::shared::TagMapping::blind::<CustomerEmail>("email"),
        ])
    }
    fn sensitive_fields() -> Vec<::skilj_core::shared::SensitiveField> {
        Vec::from([
            ::skilj_core::shared::SensitiveField::new("email", "customer", "customer_id"),
        ])
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct RegisterCustomerPayload {
    pub customer_id: String,
    pub email: String,
}
pub struct RegisterCustomer;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::CommandType for RegisterCustomer {
    type Payload = RegisterCustomerPayload;
    type Event = CustomersEvent;
    const NAME: &'static str = "RegisterCustomer";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        Vec::from([
            ::skilj_core::shared::TagMapping::new("customer", "customer_id"),
            ::skilj_core::shared::TagMapping::blind::<CustomerEmail>("email"),
        ])
    }
    fn sensitive_fields() -> Vec<::skilj_core::shared::SensitiveField> {
        Vec::from([
            ::skilj_core::shared::SensitiveField::new("email", "customer", "customer_id"),
        ])
    }
    fn consistency_query() -> Vec<::skilj_core::shared::QueryItemMapping> {
        Vec::from([
            ::skilj_core::shared::QueryItemMapping {
                event_types: Vec::from([
                    "EmailClaimed".to_string(),
                    "EmailReleased".to_string(),
                ]),
                tag_mappings: Vec::from([
                    ::skilj_core::shared::TagMapping::blind::<CustomerEmail>("email"),
                ]),
                latest: Some(1u32),
                snapshot: None,
            },
        ])
    }
    fn decide(
        payload: &Self::Payload,
        matching_events: &[Self::Event],
    ) -> ::skilj_core::shared::CommandDecision {
        decide_register_customer(payload, matching_events)
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct ChangeEmailPayload {
    pub customer_id: String,
    pub old_email: String,
    pub email: String,
}
pub struct ChangeEmail;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::CommandType for ChangeEmail {
    type Payload = ChangeEmailPayload;
    type Event = CustomersEvent;
    const NAME: &'static str = "ChangeEmail";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        Vec::from([
            ::skilj_core::shared::TagMapping::new("customer", "customer_id"),
            ::skilj_core::shared::TagMapping::blind::<CustomerEmail>("email"),
        ])
    }
    fn sensitive_fields() -> Vec<::skilj_core::shared::SensitiveField> {
        Vec::from([
            ::skilj_core::shared::SensitiveField::new(
                "old_email",
                "customer",
                "customer_id",
            ),
            ::skilj_core::shared::SensitiveField::new("email", "customer", "customer_id"),
        ])
    }
    fn consistency_query() -> Vec<::skilj_core::shared::QueryItemMapping> {
        Vec::from([
            ::skilj_core::shared::QueryItemMapping {
                event_types: Vec::from([
                    "EmailClaimed".to_string(),
                    "EmailReleased".to_string(),
                ]),
                tag_mappings: Vec::from([
                    ::skilj_core::shared::TagMapping::blind::<CustomerEmail>("email"),
                ]),
                latest: Some(1u32),
                snapshot: None,
            },
        ])
    }
    fn decide(
        payload: &Self::Payload,
        matching_events: &[Self::Event],
    ) -> ::skilj_core::shared::CommandDecision {
        decide_change_email(payload, matching_events)
    }
}
pub enum CustomersEvent {
    EmailClaimed(EmailClaimedPayload),
    EmailReleased(EmailReleasedPayload),
}
impl ::skilj_core::plugin::BoundedContextEvent for CustomersEvent {
    fn try_from_event(
        event: &::skilj_core::event_store::Event,
    ) -> Option<Result<Self, ::serde_json::Error>> {
        match event.event_type.name.as_str() {
            "EmailClaimed" => {
                Some(
                    ::serde_json::from_str(&event.payload)
                        .map(CustomersEvent::EmailClaimed),
                )
            }
            "EmailReleased" => {
                Some(
                    ::serde_json::from_str(&event.payload)
                        .map(CustomersEvent::EmailReleased),
                )
            }
            _ => None,
        }
    }
}
