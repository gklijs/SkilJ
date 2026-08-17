//! The one deliberate proc-macro exception to skilj-core's "trait-per-
//! type, no macros" plugin API (docs/architecture.md §1.3): a single
//! attribute, `#[requires_role("name")]`, applied directly above a
//! `CommandType` impl to declare an extra caller-facing role-name gate.
//!
//! Everything else about a `CommandType` - `NAME`, `decide()`,
//! `rest_trigger_allowed()`, and so on - stays a plain trait method, per
//! §1.3's own reasoning (no codegen step, `cargo doc` sees the real
//! trait). This one attribute exists because the user asked for the
//! requirement to read as an annotation on the command's own
//! declaration, not a trait method the author has to remember to
//! override - see §1.3.1 for the full design note, including why this
//! doesn't reopen macros generally.
//!
//! Not persisted anywhere (§1.3.1) - the declared name lives only in the
//! compiled binary, read back out via `CommandType::required_role()`
//! (the method this macro injects) purely to be checked by
//! `skilj-graphql`'s own mutation resolver, once that surface exists
//! (§8 item 5). REST triggering is untouched - `CommandToken` is
//! already its own, separate per-token capability grant.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemImpl, LitStr};

/// `#[requires_role("treasury_officer")]` above `impl CommandType for
/// WithdrawMoney { ... }` injects
/// `fn required_role() -> Option<&'static str> { Some("treasury_officer") }`
/// into the impl block, overriding `CommandType`'s own default
/// (`None` - no extra gate). Exactly one string-literal argument;
/// anything else is a compile error pointing at the attribute itself,
/// not a panic during macro expansion.
///
/// Only sanity-checked to be sitting on an `impl ... for ...` block whose
/// trait path's last segment is literally `CommandType` - a best-effort
/// diagnostic (an aliased import could still slip past this), not a real
/// type check; the compiler catches a genuine misuse (no such method on
/// the actual trait being implemented) right after expansion regardless.
#[proc_macro_attribute]
pub fn requires_role(attr: TokenStream, item: TokenStream) -> TokenStream {
    let role_name = parse_macro_input!(attr as LitStr);
    let mut item_impl = parse_macro_input!(item as ItemImpl);

    let implements_command_type = item_impl
        .trait_
        .as_ref()
        .and_then(|(_, path, _)| path.segments.last())
        .is_some_and(|segment| segment.ident == "CommandType");
    if !implements_command_type {
        return syn::Error::new_spanned(
            &item_impl,
            "#[requires_role(...)] only belongs on an `impl CommandType for ...` block",
        )
        .to_compile_error()
        .into();
    }

    let required_role_method: syn::ImplItemFn = syn::parse_quote! {
        fn required_role() -> Option<&'static str> {
            Some(#role_name)
        }
    };
    item_impl
        .items
        .push(syn::ImplItem::Fn(required_role_method));

    quote! { #item_impl }.into()
}
