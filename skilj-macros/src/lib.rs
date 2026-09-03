//! Three proc-macros, each scoped narrowly to a single, genuinely
//! mechanical piece of boilerplate rather than a general codegen layer -
//! see docs/architecture.md §1.3/§1.3.1/§1.3.3 for the full design note
//! behind each:
//!
//! - `#[requires_role("name")]`, applied directly above a `CommandType`
//!   impl to declare an extra caller-facing role-name gate -
//!   `skilj-core`'s plugin API.
//! - `#[auto_register]`, applied directly above an `EventType`/
//!   `CommandType`/`Projection` impl so it registers itself onto a
//!   `skilj::SkiljBuilder` without an explicit `.event_type::<T>()`/
//!   `.command_type::<T>()`/`.projection::<T>()` call - `skilj`'s own
//!   facade layer, unlike the other two (see this macro's own doc
//!   comment for why it can't live at the `skilj-core` plugin-API level
//!   the way `requires_role` does).
//! - `gql_object!(...)`, replacing the repetitive `Object::new(...).field(scalar_field(...))...`
//!   chains `skilj-graphql`'s own static `dynamic::Object` builders would
//!   otherwise hand-write one field at a time - `skilj-graphql`'s own
//!   wire-type layer.
//!
//! All three are real `#[proc_macro]`/`#[proc_macro_attribute]`s, not
//! `macro_rules!`, for the same reason: better error spans and the
//! ability to actually inspect the syntax tree (checking `requires_role`/
//! `auto_register` sit on the right kind of impl; splicing `gql_object!`'s
//! own field list into three different helper-function calls depending on
//! each field's declared kind).

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{braced, parse_macro_input, Expr, Ident, ItemImpl, LitStr, Token, Type};

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
        .and_then(|(path, _)| path.segments.last())
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

/// Applied directly above `impl EventType for X { ... }` / `impl
/// CommandType for X { ... }` / `impl Projection for X { ... }`, this
/// makes `X` register itself onto any `skilj::SkiljBuilder` that calls
/// `.auto_register()` - no explicit `.event_type::<X>()`/`.command_type::<X>()`/
/// `.projection::<X>()` call needed.
///
/// Takes an optional single expression argument - `#[auto_register(EXPR)]`
/// injects `const BOUNDED_CONTEXT: &'static str = EXPR;` into the impl
/// block, exactly as if it had been hand-written there. This is the
/// pattern a bounded-context module with its own `pub const
/// BOUNDED_CONTEXT` (`skilj-demo`'s `banking.rs`/`courses.rs`, say) uses
/// to scope every type in the file without repeating a whole const
/// declaration per type:
///
/// ```rust,ignore
/// pub const BOUNDED_CONTEXT: &str = "banking";
///
/// #[auto_register(BOUNDED_CONTEXT)]
/// impl EventType for MoneyDeposited {
///     type Payload = MoneyDepositedPayload;
///     const NAME: &'static str = "MoneyDeposited";
///     // ...
/// }
/// ```
///
/// `EXPR` is spliced verbatim, so it's not limited to a bare path - any
/// expression valid as a `const` initializer works. Bare `#[auto_register]`
/// (no argument) is unchanged: the impl's own `BOUNDED_CONTEXT` is left
/// exactly as written, falling back to the trait's own default
/// (`plugin::DEFAULT_BOUNDED_CONTEXT`, "default") if it's not overridden
/// by hand either. Writing both - the argument *and* a hand-written
/// `const BOUNDED_CONTEXT` in the impl body - is a compile error (a
/// duplicate item), not silently one-or-the-other.
///
/// Expands to the impl block plus the optional injected const (if any),
/// followed by one `inventory::submit!` registering a small
/// `fn(SkiljBuilder) -> SkiljBuilder` closure - `X::BOUNDED_CONTEXT` (see
/// that const's own doc comment on `EventType`/`CommandType`/`Projection`/
/// `Snapshot`, `skilj-core`) scopes the registration, so a type never
/// overriding it lands under `plugin::DEFAULT_BOUNDED_CONTEXT` ("default")
/// with zero other configuration anywhere. `SkiljBuilder::auto_register()`
/// folds every submitted closure across the whole linked binary, in
/// whatever order `inventory` iterates them in - order never matters,
/// since each closure only ever inserts its own `(bounded_context, NAME)`
/// entry.
///
/// **Facade-only, unlike `requires_role`.** `requires_role` expands to a
/// trait-method override alone, so it works against `skilj-core`'s plugin
/// API directly, no `skilj` dependency needed. This macro's whole point is
/// registering onto `skilj::SkiljBuilder` - the emitted `inventory::submit!`
/// necessarily names `skilj`'s own `EventTypeRegistrar`/`CommandTypeRegistrar`/
/// `ProjectionRegistrar`/`SnapshotRegistrar` types (via `::skilj::...`
/// absolute paths, so a consuming crate needs only its existing `skilj`
/// dependency, not a direct one on `inventory` too - `skilj` re-exports
/// the crate for exactly this). A consumer using `skilj-core` directly,
/// without the `skilj` facade, can't use this attribute - the same
/// boundary that consumer already accepts by hand-rolling its own
/// `CommandDispatcher`/`ProjectionDispatcher`/`EventDispatcher`/
/// `SnapshotDispatcher` (see those traits' own doc comments in
/// `skilj-core::plugin`).
///
/// Only sanity-checked to be sitting on an `impl ... for ...` block whose
/// trait path's last segment is literally `EventType`/`CommandType`/
/// `Projection`/`Snapshot` - the same best-effort diagnostic
/// `requires_role` makes for `CommandType`, not a real type check.
#[proc_macro_attribute]
pub fn auto_register(attr: TokenStream, item: TokenStream) -> TokenStream {
    let bounded_context_expr = if attr.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attr as Expr))
    };
    let mut item_impl = parse_macro_input!(item as ItemImpl);

    if let Some(expr) = bounded_context_expr {
        let already_overridden = item_impl
            .items
            .iter()
            .any(|item| matches!(item, syn::ImplItem::Const(c) if c.ident == "BOUNDED_CONTEXT"));
        if already_overridden {
            return syn::Error::new_spanned(
                &item_impl,
                "#[auto_register(...)] was given a bounded-context argument, but this impl \
                 already declares its own `const BOUNDED_CONTEXT` - remove one or the other",
            )
            .to_compile_error()
            .into();
        }
        let const_item: syn::ImplItemConst = syn::parse_quote! {
            const BOUNDED_CONTEXT: &'static str = #expr;
        };
        item_impl.items.push(syn::ImplItem::Const(const_item));
    }

    let trait_name = item_impl
        .trait_
        .as_ref()
        .and_then(|(path, _)| path.segments.last())
        .map(|segment| segment.ident.to_string());

    let self_ty = &item_impl.self_ty;
    let registration = match trait_name.as_deref() {
        Some("EventType") => quote! {
            ::skilj::inventory::submit! {
                ::skilj::EventTypeRegistrar(|b| {
                    b.bounded_context(<#self_ty as ::skilj_core::plugin::EventType>::BOUNDED_CONTEXT)
                        .event_type::<#self_ty>()
                })
            }
        },
        Some("CommandType") => quote! {
            ::skilj::inventory::submit! {
                ::skilj::CommandTypeRegistrar(|b| {
                    b.bounded_context(<#self_ty as ::skilj_core::plugin::CommandType>::BOUNDED_CONTEXT)
                        .command_type::<#self_ty>()
                })
            }
        },
        Some("Projection") => quote! {
            ::skilj::inventory::submit! {
                ::skilj::ProjectionRegistrar(|b| {
                    b.bounded_context(<#self_ty as ::skilj_core::plugin::Projection>::BOUNDED_CONTEXT)
                        .projection::<#self_ty>()
                })
            }
        },
        Some("Snapshot") => quote! {
            ::skilj::inventory::submit! {
                ::skilj::SnapshotRegistrar(|b| {
                    b.bounded_context(<#self_ty as ::skilj_core::plugin::Snapshot>::BOUNDED_CONTEXT)
                        .snapshot::<#self_ty>()
                })
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &item_impl,
                "#[auto_register] only belongs on an `impl EventType for ...` / `impl \
                 CommandType for ...` / `impl Projection for ...` / `impl Snapshot for ...` \
                 block",
            )
            .to_compile_error()
            .into();
        }
    };

    quote! {
        #item_impl
        #registration
    }
    .into()
}

/// One of `scalar`/`object`/`list` - which of `gql_types.rs`'s own
/// `scalar_field`/`object_field`/`list_field` helper a `gql_object!`
/// field expands to. Not `syn::Ident` matched inline at expansion time -
/// parsed once, up front, so an unrecognised kind is one clear error at
/// the field's own span, not a mysterious "no function named `foo_field`"
/// after expansion.
enum FieldKind {
    Scalar,
    Object,
    List,
}

/// One field inside a `gql_object!(...)` invocation - see `gql_object`'s
/// own doc comment for the grammar. `ty` is parsed as a plain `syn::Expr`,
/// not further inspected - splicing it back out verbatim is what lets a
/// field's own resolver be an arbitrarily complex expression (a
/// multi-arm `match`, say), the same freedom every hand-written
/// `scalar_field(...)` call already had. `closure` is parsed specifically
/// as `syn::ExprClosure`, not `Expr` - `gql_object`'s own expansion needs
/// to reach into its one parameter and give it a real type ascription
/// (plain type inference alone can't resolve a bare `|r| ...` passed to
/// a generic `Fn(&T) -> _` parameter - see `gql_object`'s own doc
/// comment), which means owning the closure's own AST, not just its
/// token stream.
struct GqlField {
    kind: FieldKind,
    name: LitStr,
    ty: Expr,
    closure: syn::ExprClosure,
}

impl Parse for GqlField {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let kind_ident: Ident = input.parse()?;
        let kind = match kind_ident.to_string().as_str() {
            "scalar" => FieldKind::Scalar,
            "object" => FieldKind::Object,
            "list" => FieldKind::List,
            other => {
                return Err(syn::Error::new(
                    kind_ident.span(),
                    format!("expected `scalar`, `object`, or `list`, found `{other}`"),
                ))
            }
        };
        let name: LitStr = input.parse()?;
        input.parse::<Token![:]>()?;
        // `=>`, not `=` - `=` would make the type expression's own parse
        // ambiguous with `syn::Expr::Assign` (`TypeRef::named(...) = ...`
        // is itself a valid, if nonsensical, expression `syn::Expr` would
        // happily consume whole). `=>` never continues a plain
        // expression, so parsing the type stops cleanly right before it.
        let ty: Expr = input.parse()?;
        input.parse::<Token![=>]>()?;
        let closure: syn::ExprClosure = input.parse()?;
        if closure.inputs.len() != 1 {
            return Err(syn::Error::new_spanned(
                &closure,
                "gql_object! field closures take exactly one parameter",
            ));
        }
        Ok(GqlField {
            kind,
            name,
            ty,
            closure,
        })
    }
}

/// `gql_object!(RustType => "GraphQLName" { field, field, ... })` - the
/// full invocation `gql_object!` itself parses into.
struct GqlObjectInput {
    rust_type: Type,
    gql_name: LitStr,
    fields: syn::punctuated::Punctuated<GqlField, Token![,]>,
}

impl Parse for GqlObjectInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let rust_type: Type = input.parse()?;
        input.parse::<Token![=>]>()?;
        let gql_name: LitStr = input.parse()?;
        let content;
        braced!(content in input);
        let fields = content.parse_terminated(GqlField::parse, Token![,])?;
        Ok(GqlObjectInput {
            rust_type,
            gql_name,
            fields,
        })
    }
}

/// Expands to an `async_graphql::dynamic::Object` expression - the same
/// value every hand-written `Object::new("X").field(scalar_field(...))...`
/// chain in `skilj-graphql/src/gql_types.rs` already builds, just from a
/// shorter declaration. Meant to sit directly inside that crate's own
/// `pub fn x_object() -> Object { ... }` wrapper - the function name,
/// its doc comment, and its signature all stay hand-written and
/// unaffected; only the repetitive body is generated.
///
/// ```ignore
/// pub fn role_object() -> Object {
///     gql_object!(Role => "Role" {
///         scalar "id": TypeRef::named_nn(TypeRef::ID) => |r| Value::from(r.id.clone()),
///         scalar "revokedAt": TypeRef::named(TypeRef::STRING) => |r| optional_timestamp(r.revoked_at),
///         object "role": TypeRef::named_nn("Role") => |m| Some(m.role.clone()),
///         list "tagMappings": TypeRef::named_nn_list_nn("TagMapping") => |et| et.tag_mappings.clone(),
///     })
/// }
/// ```
///
/// Each field's own closure is deliberately left bare (`|r| ...`, no
/// `: &Role` annotation) - the macro already knows `Role` from the
/// invocation's own `RustType =>` and splices it into the closure's own
/// one parameter directly (`|r: &Role| ...`) before re-emitting it.
/// Plain type inference can't do this on its own: `scalar_field`'s
/// `resolve` parameter is generic (`F: Fn(&T) -> Value`), so a bare `|r|
/// ...` passed there leaves `T` and `r`'s own type circularly unresolved,
/// so the macro has to give the parameter a real type ascription, the
/// same one every hand-written call already spelled out on every single
/// field. `scalar_field`/`object_field`/`list_field` themselves are
/// unqualified identifiers in the generated code, resolved at the *call
/// site*'s own scope (this is a `#[proc_macro]`, call-site hygiene
/// applies) - they stay `gql_types.rs`'s own private helpers, never
/// exposed to or duplicated by this crate.
#[proc_macro]
pub fn gql_object(input: TokenStream) -> TokenStream {
    let GqlObjectInput {
        rust_type,
        gql_name,
        fields,
    } = parse_macro_input!(input as GqlObjectInput);

    let field_exprs = fields.into_iter().map(|field| {
        let GqlField {
            kind,
            name,
            ty,
            mut closure,
        } = field;
        let helper = match kind {
            FieldKind::Scalar => quote! { scalar_field },
            FieldKind::Object => quote! { object_field },
            FieldKind::List => quote! { list_field },
        };
        // `closure.inputs.len() == 1` already checked in `GqlField::parse`
        // - swap that one bare parameter for the same pattern, retyped to
        // `&RustType`.
        let param =
            closure.inputs.first().cloned().expect(
                "GqlField::parse already rejected any closure without exactly one parameter",
            );
        let typed_param = syn::Pat::Type(syn::PatType {
            attrs: Vec::new(),
            pat: Box::new(param),
            colon_token: syn::token::Colon::default(),
            ty: Box::new(syn::parse_quote!(&#rust_type)),
        });
        closure.inputs = syn::punctuated::Punctuated::new();
        closure.inputs.push(typed_param);
        quote! {
            .field(#helper(#name, #ty, #closure))
        }
    });

    quote! {
        Object::new(#gql_name)
            #(#field_exprs)*
    }
    .into()
}
