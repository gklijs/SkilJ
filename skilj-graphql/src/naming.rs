//! The names skilj's GraphQL schema gives its own types and root fields,
//! under an optional prefix (docs/architecture.md §194). A federation
//! subgraph's types and root fields share one namespace with every other
//! subgraph's, so a second skilj service, or another team's `Role`,
//! would otherwise fail composition. Without a prefix every name is the
//! one skilj has always used.
//!
//! Only the names skilj itself declares go through here: object, enum,
//! input and union types, and the fields of `Query`, `Mutation` and
//! `Subscription`. The root types keep their names (every subgraph's
//! `Query` merges into the supergraph's), and so do fields nested under a
//! type, since the type's own name already sets them apart.

/// See the module doc comment.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Naming {
    /// Empty, or a valid prefix (see [`Naming::new`]).
    prefix: String,
}

impl Naming {
    /// `prefix` must be a GraphQL name of ASCII letters and digits,
    /// starting with a letter: it is prepended to type names with its
    /// first letter in upper case (`ledger` → `LedgerRole`) and to root
    /// field names in lower case (`ledgerQueryEvents`). No `_`, so a
    /// prefixed name can never start with the `__` GraphQL reserves. An
    /// empty prefix leaves every name as it is.
    pub fn new(prefix: &str) -> Result<Self, String> {
        let mut chars = prefix.chars();
        let valid = match chars.next() {
            None => true,
            Some(first) => first.is_ascii_alphabetic() && chars.all(|c| c.is_ascii_alphanumeric()),
        };
        if !valid {
            return Err(format!(
                "GraphQL name prefix {prefix:?} must be ASCII letters and digits, starting with \
                 a letter"
            ));
        }
        Ok(Self {
            prefix: prefix.to_string(),
        })
    }

    /// The prefix as configured; empty when there is none.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The name of one of skilj's own types.
    pub fn ty(&self, name: &str) -> String {
        if self.prefix.is_empty() {
            return name.to_string();
        }
        format!(
            "{}{name}",
            with_first(&self.prefix, char::to_ascii_uppercase)
        )
    }

    /// The name of one of skilj's fields on `Query`, `Mutation` or
    /// `Subscription`.
    pub fn root(&self, name: &str) -> String {
        if self.prefix.is_empty() {
            return name.to_string();
        }
        format!(
            "{}{}",
            with_first(&self.prefix, char::to_ascii_lowercase),
            with_first(name, char::to_ascii_uppercase)
        )
    }
}

fn with_first(s: &str, f: impl Fn(&char) -> char) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => std::iter::once(f(&first)).chain(chars).collect(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::Naming;

    #[test]
    fn no_prefix_leaves_names_alone() {
        let n = Naming::default();
        assert_eq!(n.ty("Role"), "Role");
        assert_eq!(n.root("queryEvents"), "queryEvents");
    }

    #[test]
    fn a_prefix_goes_in_front_of_types_and_root_fields() {
        let n = Naming::new("ledger").unwrap();
        assert_eq!(n.ty("Role"), "LedgerRole");
        assert_eq!(
            n.ty("banking_AccountBalance"),
            "Ledgerbanking_AccountBalance"
        );
        assert_eq!(n.root("queryEvents"), "ledgerQueryEvents");
        let n = Naming::new("Ledger2").unwrap();
        assert_eq!(n.ty("Role"), "Ledger2Role");
        assert_eq!(n.root("epoch"), "ledger2Epoch");
    }

    #[test]
    fn a_prefix_must_be_letters_and_digits_starting_with_a_letter() {
        for bad in ["_x", "2x", "led-ger", "led_ger", "lédger"] {
            assert!(Naming::new(bad).is_err(), "{bad}");
        }
    }
}
