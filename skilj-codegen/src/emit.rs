//! `BoundedContextSpec` -> real Rust source, via `quote!`. See this
//! crate's own root doc comment/`spec.rs` for what's in and out of
//! scope. Every generated item uses fully-qualified paths
//! (`::serde::Serialize`, not `Serialize`) deliberately - the emitted
//! code is spliced into a consumer's own module via `include!()`, and
//! shouldn't depend on that module happening to have the right `use`
//! statements in scope.

use crate::spec::{BoundedContextSpec, CommandTypeSpec, EventTypeSpec, FieldSpec, FieldType};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::BTreeMap;

pub fn emit(spec: &BoundedContextSpec) -> TokenStream {
    let bc_name = &spec.bounded_context;
    let bc_const = quote! {
        pub const BOUNDED_CONTEXT: &str = #bc_name;
    };

    let event_enum_ident = format_ident!("{}Event", snake_case_to_pascal_case(bc_name));

    let event_items = spec.event_types.iter().map(emit_event_type);
    let command_items = spec
        .command_types
        .iter()
        .map(|c| emit_command_type(c, &event_enum_ident));
    let event_enum = emit_event_enum(spec, &event_enum_ident);

    quote! {
        #bc_const
        #(#event_items)*
        #(#command_items)*
        #event_enum
    }
}

fn field_type_tokens(ty: FieldType) -> TokenStream {
    match ty {
        FieldType::String => quote! { String },
        FieldType::I64 => quote! { i64 },
        FieldType::Bool => quote! { bool },
    }
}

/// The payload struct shared by both an event type and a command type -
/// `#[derive(...)]` list matches every hand-written payload struct in
/// `skilj-demo` exactly (`Debug, Clone, Serialize, Deserialize,
/// JsonSchema`), since that's the real bound the plugin API's own
/// `type Payload: Serialize + DeserializeOwned + JsonSchema` requires
/// (`Clone`/`Debug` aren't required by the trait itself, but every real
/// payload struct in this codebase has them, and `EventSpec`/
/// `CommandDecision` construction wants `Clone` in practice).
fn payload_struct(type_name: &str, fields: &[FieldSpec]) -> (syn::Ident, TokenStream) {
    let struct_ident = format_ident!("{type_name}Payload");
    let field_tokens = fields.iter().map(|f| {
        let field_ident = format_ident!("{}", f.name);
        let ty = field_type_tokens(f.ty);
        quote! { pub #field_ident: #ty }
    });
    let tokens = quote! {
        #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, ::schemars::JsonSchema)]
        pub struct #struct_ident {
            #(#field_tokens),*
        }
    };
    (struct_ident, tokens)
}

/// `None` when `tags` is empty - the trait's own default
/// (`Vec::new()`) already covers that case, so nothing needs
/// overriding; emitting an override anyway would just be a longer way
/// to say the same thing.
fn tag_mappings_fn(tags: &BTreeMap<String, String>) -> Option<TokenStream> {
    if tags.is_empty() {
        return None;
    }
    let entries = tags.iter().map(|(key, field)| {
        quote! { ::skilj_core::shared::TagMapping { key: #key.to_string(), field: #field.to_string() } }
    });
    Some(quote! {
        fn tag_mappings() -> Vec<::skilj_core::shared::TagMapping> {
            vec![#(#entries),*]
        }
    })
}

fn emit_event_type(event: &EventTypeSpec) -> TokenStream {
    let (payload_ident, payload_tokens) = payload_struct(&event.name, &event.fields);
    let marker_ident = format_ident!("{}", event.name);
    let name_lit = &event.name;
    let tag_fn = tag_mappings_fn(&event.tags);

    quote! {
        #payload_tokens

        pub struct #marker_ident;

        #[::skilj::auto_register(BOUNDED_CONTEXT)]
        impl ::skilj::EventType for #marker_ident {
            type Payload = #payload_ident;
            const NAME: &'static str = #name_lit;
            #tag_fn
        }
    }
}

/// `decide()` is generated as a one-line delegation to a hand-written
/// free function, `decide_<snake_case(NAME)>` - the including module is
/// expected to already define it (the same `.skilj.toml`-adjacent Rust
/// file this codegen's own output is spliced into via `include!`, per
/// `skilj-demo/build.rs`'s own doc comment). A missing one is a real,
/// immediate "cannot find function" compile error in the including
/// crate - never a silent gap, and never something this crate itself
/// needs to detect ahead of time.
fn emit_command_type(command: &CommandTypeSpec, event_enum_ident: &syn::Ident) -> TokenStream {
    let (payload_ident, payload_tokens) = payload_struct(&command.name, &command.fields);
    let marker_ident = format_ident!("{}", command.name);
    let name_lit = &command.name;
    let tag_fn = tag_mappings_fn(&command.tags);
    let rest_fn = command.rest_trigger_allowed.then(|| {
        quote! {
            fn rest_trigger_allowed() -> bool { true }
        }
    });
    let decide_fn_ident = format_ident!("decide_{}", pascal_case_to_snake_case(&command.name));

    quote! {
        #payload_tokens

        pub struct #marker_ident;

        #[::skilj::auto_register(BOUNDED_CONTEXT)]
        impl ::skilj::CommandType for #marker_ident {
            type Payload = #payload_ident;
            type Event = #event_enum_ident;
            const NAME: &'static str = #name_lit;
            #tag_fn
            #rest_fn
            fn decide(
                payload: &Self::Payload,
                matching_events: &[Self::Event],
            ) -> ::skilj_core::shared::CommandDecision {
                #decide_fn_ident(payload, matching_events)
            }
        }
    }
}

/// The shared per-bounded-context event enum + its `BoundedContextEvent`
/// impl - Finding 1 from the [§16](../../docs/architecture.md#declarative-bounded-context-codegen-prototype) prototype: entirely mechanical, one
/// variant/match-arm per event type, with zero hand-judgement involved.
fn emit_event_enum(spec: &BoundedContextSpec, enum_ident: &syn::Ident) -> TokenStream {
    let variants = spec.event_types.iter().map(|e| {
        let variant_ident = format_ident!("{}", e.name);
        let payload_ident = format_ident!("{}Payload", e.name);
        quote! { #variant_ident(#payload_ident) }
    });
    let match_arms = spec.event_types.iter().map(|e| {
        let variant_ident = format_ident!("{}", e.name);
        let name_lit = &e.name;
        quote! {
            #name_lit => {
                Some(::serde_json::from_str(&event.payload).map(#enum_ident::#variant_ident))
            }
        }
    });

    quote! {
        pub enum #enum_ident {
            #(#variants),*
        }

        impl ::skilj_core::plugin::BoundedContextEvent for #enum_ident {
            fn try_from_event(
                event: &::skilj_core::event_store::Event,
            ) -> Option<Result<Self, ::serde_json::Error>> {
                match event.event_type.name.as_str() {
                    #(#match_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// "DepositMoney" -> "deposit_money" - the naming convention
/// `emit_command_type`'s own generated `decide()` delegates through.
fn pascal_case_to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                result.push('_');
            }
            result.extend(c.to_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

/// "banking" -> "Banking", "my_context" -> "MyContext" - the shared
/// event enum's own name (`<PascalCase(bounded_context)>Event`).
fn snake_case_to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pascal_to_snake_case_handles_a_multi_word_name() {
        assert_eq!(pascal_case_to_snake_case("DepositMoney"), "deposit_money");
        assert_eq!(
            pascal_case_to_snake_case("MoneyDeposited"),
            "money_deposited"
        );
    }

    #[test]
    fn snake_to_pascal_case_handles_a_single_and_multi_word_name() {
        assert_eq!(snake_case_to_pascal_case("banking"), "Banking");
        assert_eq!(snake_case_to_pascal_case("my_context"), "MyContext");
    }
}
