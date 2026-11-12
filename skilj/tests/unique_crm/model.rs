//! A model of the unique-email domain, for checking generated command
//! histories against (issue #189). It is written from the rules in this
//! module's parent doc and from the spec's `ProcessCommand` and retraction
//! rules, never from `ValueClaims`, `claim_query` or `correction_query`: its
//! state is the whole log, and who holds an address is worked out from all of
//! it. The store and the domain see only what each command's consistency
//! query reads, so a disagreement between the two is the kind of bug this is
//! here to find.
//!
//! Operations name customers, addresses and targets by small indices, and a
//! target is resolved against the history at the time it is applied, so a
//! history stays valid while it shrinks.

use std::collections::{HashMap, HashSet};

/// The customers a history uses.
pub const CUSTOMERS: u8 = 3;
/// The distinct addresses, once normalized.
pub const BASES: u8 = 4;
/// The spellings of each: the same address for a blind tag, which
/// normalizes, and distinct ones for a plain tag, which doesn't.
pub const SPELLINGS: u8 = 3;
/// The idempotency keys a history draws from, so they get reused.
pub const KEYS: u8 = 3;
/// The callers, each with its own idempotency keys.
pub const CLIENTS: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Variant {
    Blind,
    Plain,
}

/// An address, as one of its spellings. The spellings differ only in what
/// `normalize::email` documents it removes: case and surrounding whitespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Addr {
    pub base: u8,
    pub spelling: u8,
}

impl Addr {
    pub fn text(self) -> String {
        let base = self.base % BASES;
        match self.spelling % SPELLINGS {
            0 => format!("a{base}@example.test"),
            1 => format!("A{base}@Example.TEST"),
            _ => format!(" a{base}@example.test\t"),
        }
    }

    /// The canonical spelling, which is what a blind tag hashes.
    pub fn canonical(self) -> String {
        format!("a{}@example.test", self.base % BASES)
    }
}

/// The address a tag stands for: the base alone when the tag is blind, the
/// spelling too when it is plain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Value {
    pub base: u8,
    pub spelling: u8,
}

impl Value {
    pub fn of(variant: Variant, addr: Addr) -> Self {
        Value {
            base: addr.base % BASES,
            spelling: match variant {
                Variant::Blind => 0,
                Variant::Plain => addr.spelling % SPELLINGS,
            },
        }
    }

    pub fn text(self) -> String {
        Addr {
            base: self.base,
            spelling: self.spelling,
        }
        .text()
    }
}

/// Who submits: a client and maybe an idempotency key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Call {
    pub client: u8,
    pub key: Option<u8>,
}

impl Call {
    pub fn client_id(self) -> String {
        format!("client-{}", self.client % CLIENTS)
    }

    pub fn key(self) -> Option<String> {
        self.key.map(|key| format!("key-{}", key % KEYS))
    }
}

/// Which event an operator names, resolved against the history so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The nth release in the history, or the nth event when there is none.
    Release(u16),
    /// The nth event in the history that may be retracted without changing
    /// who holds what: a claim, a retraction, anything but a release.
    NotARelease(u16),
    /// A position past the end of the history.
    PastEnd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Register {
        customer: u8,
        addr: Addr,
        call: Call,
    },
    Correct {
        customer: u8,
        addr: Addr,
        call: Call,
    },
    Release {
        customer: u8,
        addr: Addr,
        call: Call,
    },
    /// The customer and address default to the target's own, as an
    /// operator naming them right would give them.
    RetractRelease {
        customer: Option<u8>,
        addr: Option<Addr>,
        target: Target,
        call: Call,
    },
    RetractAny {
        target: Target,
        call: Call,
    },
}

impl Op {
    pub fn command_type(&self) -> &'static str {
        match self {
            Op::Register { .. } => "RegisterCustomer",
            Op::Correct { .. } => "CorrectEmail",
            Op::Release { .. } => "ReleaseEmail",
            Op::RetractRelease { .. } => "RetractRelease",
            Op::RetractAny { .. } => "RetractAny",
        }
    }

    pub fn call(&self) -> Call {
        match self {
            Op::Register { call, .. }
            | Op::Correct { call, .. }
            | Op::Release { call, .. }
            | Op::RetractRelease { call, .. }
            | Op::RetractAny { call, .. } => *call,
        }
    }
}

pub fn customer_id(customer: u8) -> String {
    format!("c{}", customer % CUSTOMERS)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Claimed,
    Released,
    /// Retracts the event at that index of the log.
    Retraction(usize),
}

/// An event of the log, with the tags that matter: a retraction carries its
/// target's, as the store copies them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Logged {
    pub kind: Kind,
    pub customer: u8,
    pub value: Value,
}

/// What an operation must answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expected {
    /// These events appended, in this order.
    Accepted(Vec<Logged>),
    Rejected(&'static str),
    /// The events of the log at these indices, recorded under the key.
    Deduplicated(Vec<usize>),
}

/// Where a resolved target is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position {
    Index(usize),
    PastEnd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub expected: Expected,
    /// The event an operator op names, resolved.
    pub target: Option<Position>,
    /// The customer and address a [`Op::RetractRelease`] submits, resolved.
    pub release_args: Option<(u8, Addr)>,
}

#[derive(Debug)]
pub struct Model {
    pub variant: Variant,
    pub log: Vec<Logged>,
    /// Accepted keys: (command type, client, key) to what they appended.
    recorded: HashMap<(&'static str, u8, u8), Vec<usize>>,
}

impl Model {
    pub fn new(variant: Variant) -> Self {
        Model {
            variant,
            log: Vec::new(),
            recorded: HashMap::new(),
        }
    }

    fn value(&self, addr: Addr) -> Value {
        Value::of(self.variant, addr)
    }

    fn retracted(&self) -> HashSet<usize> {
        self.log
            .iter()
            .filter_map(|event| match event.kind {
                Kind::Retraction(target) => Some(target),
                _ => None,
            })
            .collect()
    }

    /// The claims and releases still endorsed, with their indices.
    fn standing(&self) -> impl Iterator<Item = (usize, &Logged)> {
        let retracted = self.retracted();
        self.log.iter().enumerate().filter(move |(index, event)| {
            !retracted.contains(index) && !matches!(event.kind, Kind::Retraction(_))
        })
    }

    /// Who holds `value`: the customer of its latest standing claim, nobody
    /// when its latest standing event is a release or it has none.
    pub fn holder(&self, value: Value) -> Option<(u8, usize)> {
        let latest = self
            .standing()
            .filter(|(_, event)| event.value == value)
            .last()?;
        (latest.1.kind == Kind::Claimed).then_some((latest.1.customer, latest.0))
    }

    /// The address `customer` holds and the index of its claim. The domain
    /// holds a customer to one; more than one is a broken model or store.
    pub fn held_by(&self, customer: u8) -> Option<(Value, usize)> {
        let mut values: Vec<Value> = Vec::new();
        for (_, event) in self.standing() {
            if !values.contains(&event.value) {
                values.push(event.value);
            }
        }
        let held: Vec<(Value, usize)> = values
            .into_iter()
            .filter_map(|value| match self.holder(value) {
                Some((holder, index)) if holder == customer => Some((value, index)),
                _ => None,
            })
            .collect();
        assert!(
            held.len() <= 1,
            "the model has customer {customer} holding {held:?}"
        );
        held.into_iter().next()
    }

    /// Every address held, and by whom: the state a history ends in.
    pub fn holders(&self) -> Vec<(Value, u8)> {
        let mut values: Vec<Value> = Vec::new();
        for (_, event) in self.standing() {
            if !values.contains(&event.value) {
                values.push(event.value);
            }
        }
        values
            .into_iter()
            .filter_map(|value| self.holder(value).map(|(customer, _)| (value, customer)))
            .collect()
    }

    fn resolve(&self, target: Target) -> Position {
        let pick = |candidates: Vec<usize>, nth: u16| {
            if candidates.is_empty() {
                Position::PastEnd
            } else {
                Position::Index(candidates[nth as usize % candidates.len()])
            }
        };
        match target {
            Target::PastEnd => Position::PastEnd,
            Target::Release(nth) => {
                let releases: Vec<usize> = (0..self.log.len())
                    .filter(|index| self.log[*index].kind == Kind::Released)
                    .collect();
                if releases.is_empty() {
                    pick((0..self.log.len()).collect(), nth)
                } else {
                    pick(releases, nth)
                }
            }
            Target::NotARelease(nth) => pick(
                (0..self.log.len())
                    .filter(|index| self.log[*index].kind != Kind::Released)
                    .collect(),
                nth,
            ),
        }
    }

    /// What `op` must answer, applied to the model when it is accepted.
    pub fn apply(&mut self, op: Op) -> Step {
        let target = match op {
            Op::RetractRelease { target, .. } | Op::RetractAny { target, .. } => {
                Some(self.resolve(target))
            }
            _ => None,
        };
        let release_args = match op {
            Op::RetractRelease { customer, addr, .. } => {
                let named = match target {
                    Some(Position::Index(index)) => Some(self.log[index]),
                    _ => None,
                };
                Some((
                    customer.or(named.map(|event| event.customer)).unwrap_or(0) % CUSTOMERS,
                    addr.or(named.map(|event| Addr {
                        base: event.value.base,
                        spelling: event.value.spelling,
                    }))
                    .unwrap_or(Addr {
                        base: 0,
                        spelling: 0,
                    }),
                ))
            }
            _ => None,
        };
        let call = op.call();
        let scope = call
            .key
            .map(|key| (op.command_type(), call.client % CLIENTS, key % KEYS));
        // ProcessCommand: an accepted key answers with what it appended,
        // whatever the payload, and nothing is decided.
        if let Some(recorded) = scope.and_then(|scope| self.recorded.get(&scope)) {
            return Step {
                expected: Expected::Deduplicated(recorded.clone()),
                target,
                release_args,
            };
        }
        let expected = self.decide(op, target, release_args);
        if let Expected::Accepted(events) = &expected {
            let first = self.log.len();
            self.log.extend(events.iter().copied());
            if let Some(scope) = scope {
                self.recorded
                    .insert(scope, (first..self.log.len()).collect());
            }
        }
        Step {
            expected,
            target,
            release_args,
        }
    }

    fn decide(
        &self,
        op: Op,
        target: Option<Position>,
        release_args: Option<(u8, Addr)>,
    ) -> Expected {
        match op {
            Op::Register { customer, addr, .. } => {
                let customer = customer % CUSTOMERS;
                let value = self.value(addr);
                if matches!(self.holder(value), Some((holder, _)) if holder != customer) {
                    return Expected::Rejected("email_taken");
                }
                if self.held_by(customer).is_some() {
                    return Expected::Rejected("already_holds_a_value");
                }
                Expected::Accepted(vec![Logged {
                    kind: Kind::Claimed,
                    customer,
                    value,
                }])
            }
            Op::Correct { customer, addr, .. } => {
                let customer = customer % CUSTOMERS;
                let value = self.value(addr);
                if matches!(self.holder(value), Some((holder, _)) if holder != customer) {
                    return Expected::Rejected("email_taken");
                }
                let Some((old, claim)) = self.held_by(customer) else {
                    return Expected::Rejected("nothing_to_correct");
                };
                Expected::Accepted(vec![
                    Logged {
                        kind: Kind::Retraction(claim),
                        customer,
                        value: old,
                    },
                    Logged {
                        kind: Kind::Claimed,
                        customer,
                        value,
                    },
                ])
            }
            Op::Release { customer, addr, .. } => {
                let customer = customer % CUSTOMERS;
                let value = self.value(addr);
                if !matches!(self.holder(value), Some((holder, _)) if holder == customer) {
                    return Expected::Rejected("not_held");
                }
                Expected::Accepted(vec![Logged {
                    kind: Kind::Released,
                    customer,
                    value,
                }])
            }
            Op::RetractRelease { .. } => {
                let (customer, addr) = release_args.expect("resolved in apply");
                let payload_value = self.value(addr);
                let retracted = self.retracted();
                let release = match target {
                    Some(Position::Index(index))
                        if self.log[index].kind == Kind::Released
                            && self.log[index].customer == customer
                            && !retracted.contains(&index) =>
                    {
                        index
                    }
                    _ => return Expected::Rejected("not_a_release"),
                };
                // What the decision reads of other customers is the payload
                // address's history, so that is what this is about.
                if self.standing().any(|(index, event)| {
                    index > release
                        && event.kind == Kind::Claimed
                        && event.customer != customer
                        && event.value == payload_value
                }) {
                    return Expected::Rejected("value_taken_since");
                }
                let released = self.log[release];
                if matches!(self.held_by(customer), Some((held, _)) if held != released.value) {
                    return Expected::Rejected("already_holds_a_value");
                }
                // requiring_target_tags(.., ["email"]), checked by the store.
                if released.value != payload_value {
                    return Expected::Rejected("retraction_target_mismatch");
                }
                Expected::Accepted(vec![Logged {
                    kind: Kind::Retraction(release),
                    customer: released.customer,
                    value: released.value,
                }])
            }
            Op::RetractAny { .. } => {
                let index = match target {
                    Some(Position::Index(index)) => index,
                    _ => return Expected::Rejected("retraction_target_not_found"),
                };
                let event = self.log[index];
                if matches!(event.kind, Kind::Retraction(_)) {
                    return Expected::Rejected("retraction_of_retraction");
                }
                if self.retracted().contains(&index) {
                    return Expected::Rejected("already_retracted");
                }
                Expected::Accepted(vec![Logged {
                    kind: Kind::Retraction(index),
                    customer: event.customer,
                    value: event.value,
                }])
            }
        }
    }
}
