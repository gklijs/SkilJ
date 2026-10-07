pub const BOUNDED_CONTEXT: &str = "banking";
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct MoneyDepositedPayload {
    pub account_id: String,
    pub amount: i64,
}
pub struct MoneyDeposited;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        vec![
            ::skilj_core::shared::TagMapping { key : "account".to_string(), field :
            "account_id".to_string() }
        ]
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct MoneyWithdrawnPayload {
    pub account_id: String,
    pub amount: i64,
}
pub struct MoneyWithdrawn;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::EventType for MoneyWithdrawn {
    type Payload = MoneyWithdrawnPayload;
    const NAME: &'static str = "MoneyWithdrawn";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        vec![
            ::skilj_core::shared::TagMapping { key : "account".to_string(), field :
            "account_id".to_string() }
        ]
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct DepositMoneyPayload {
    pub account_id: String,
    pub amount: i64,
}
pub struct DepositMoney;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::CommandType for DepositMoney {
    type Payload = DepositMoneyPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "DepositMoney";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        vec![
            ::skilj_core::shared::TagMapping { key : "account".to_string(), field :
            "account_id".to_string() }
        ]
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(
        payload: &Self::Payload,
        matching_events: &[Self::Event],
    ) -> ::skilj_core::shared::CommandDecision {
        decide_deposit_money(payload, matching_events)
    }
}
#[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
pub struct WithdrawMoneyPayload {
    pub account_id: String,
    pub amount: i64,
}
pub struct WithdrawMoney;
#[::skilj::auto_register(BOUNDED_CONTEXT)]
impl ::skilj::CommandType for WithdrawMoney {
    type Payload = WithdrawMoneyPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
        vec![
            ::skilj_core::shared::TagMapping { key : "account".to_string(), field :
            "account_id".to_string() }
        ]
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(
        payload: &Self::Payload,
        matching_events: &[Self::Event],
    ) -> ::skilj_core::shared::CommandDecision {
        decide_withdraw_money(payload, matching_events)
    }
}
pub enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
    MoneyWithdrawn(MoneyWithdrawnPayload),
}
impl ::skilj_core::plugin::BoundedContextEvent for BankingEvent {
    fn try_from_event(
        event: &::skilj_core::event_store::Event,
    ) -> Option<Result<Self, ::serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(
                    ::serde_json::from_str(&event.payload)
                        .map(BankingEvent::MoneyDeposited),
                )
            }
            "MoneyWithdrawn" => {
                Some(
                    ::serde_json::from_str(&event.payload)
                        .map(BankingEvent::MoneyWithdrawn),
                )
            }
            _ => None,
        }
    }
}
