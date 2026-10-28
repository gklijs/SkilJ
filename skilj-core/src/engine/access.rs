//! Identity resolution, Roles, grants, capabilities, tokens, private-field
//! grants, the access trail and subject erasure.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Row, Transaction};

use super::render::{is_default_private_reader, Record};
use super::rows::*;
use super::Skilj;
use crate::crypto;
use crate::error::{Error, Result};
use crate::model::*;
use crate::validate;

/// A freshly minted token: the only time its secret is visible.
#[derive(Debug, Clone, Serialize)]
pub struct MintedToken {
    pub token: AccessToken,
    /// Present as `Authorization: Bearer <id>.<secret>`.
    pub bearer: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AccessHistoryAnswer {
    pub known: bool,
    pub held: bool,
    pub level: Option<AccessLevel>,
    pub capabilities: BTreeSet<String>,
    pub delegable: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityOverviewRow {
    pub bounded_context: String,
    pub required: BTreeSet<String>,
    pub held: BTreeSet<String>,
    pub required_not_held: BTreeSet<String>,
    pub held_not_required: BTreeSet<String>,
}

pub(crate) const ACCESS_EVENT_TYPES: [&str; 8] = [
    "AccessGranted",
    "AccessRevoked",
    "CapabilityGranted",
    "CapabilityRevoked",
    "DelegableCapabilitiesSet",
    "AccessBaselined",
    "PrivateFieldAccessGranted",
    "PrivateFieldAccessRevoked",
];

impl Skilj {
    // ------------------------------------------------------- identity

    pub async fn role_by_subject(&self, external_subject: &str) -> Result<Option<Role>> {
        let row = sqlx::query(&format!(
            "select {ROLE_COLUMNS} from roles where external_subject = $1 and active"
        ))
        .bind(external_subject)
        .fetch_optional(&self.inner.pool)
        .await?;
        row.as_ref().map(role_from_row).transpose()
    }

    pub async fn role_by_id(&self, id: &str) -> Result<Role> {
        let row = sqlx::query(&format!("select {ROLE_COLUMNS} from roles where id = $1"))
            .bind(id)
            .fetch_optional(&self.inner.pool)
            .await?
            .ok_or_else(|| Error::not_found(format!("role {id}")))?;
        role_from_row(&row)
    }

    /// The Role's active grant on a context, resolved live.
    pub async fn grant_for_role(&self, role: &Role, bc: &str) -> Result<Option<Grant>> {
        if !role.active {
            return Ok(None);
        }
        let row = sqlx::query(&format!(
            "select {MAPPING_COLUMNS} from access_mappings where role_id = $1 and bounded_context = $2 and active"
        ))
        .bind(&role.id)
        .bind(bc)
        .fetch_optional(&self.inner.pool)
        .await?;
        Ok(row
            .as_ref()
            .map(mapping_from_row)
            .transpose()?
            .map(|mapping| Grant {
                mapping,
                role: role.clone(),
            }))
    }

    /// GraphQL identity resolution: verified subject claim -> active Role
    /// -> its active grant on the context. A context the caller has no
    /// grant on is answered as not existing.
    pub async fn resolve_grant(&self, external_subject: &str, bc: &str) -> Result<Grant> {
        let role = self
            .role_by_subject(external_subject)
            .await?
            .ok_or_else(|| Error::not_found(format!("bounded context {bc}")))?;
        self.grant_for_role(&role, bc)
            .await?
            .ok_or_else(|| Error::not_found(format!("bounded context {bc}")))
    }

    /// Resolve a superadmin caller.
    pub async fn resolve_superadmin(&self, external_subject: &str) -> Result<Role> {
        match self.role_by_subject(external_subject).await? {
            Some(r) if r.superadmin => Ok(r),
            Some(_) => Err(Error::forbidden("needs a superadmin role")),
            None => Err(Error::forbidden("unknown identity")),
        }
    }

    pub(crate) async fn reload_grant(&self, grant: &Grant) -> Result<Option<Grant>> {
        let role = self.role_by_id(&grant.role.id).await?;
        let g = self
            .grant_for_role(&role, &grant.mapping.bounded_context)
            .await?;
        Ok(g.filter(|g| g.mapping.id == grant.mapping.id))
    }

    /// REST: "<id>.<secret>" -> the token row, secret checked in constant time.
    pub async fn authenticate_token(&self, bearer: &str) -> Result<AccessToken> {
        let (id, secret) = bearer
            .split_once('.')
            .ok_or_else(|| Error::forbidden("malformed bearer token"))?;
        let row = sqlx::query(&format!(
            "select secret_hash, {TOKEN_COLUMNS} from tokens where id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| Error::forbidden("unknown token"))?;
        let hash: String = row.try_get("secret_hash")?;
        if !crypto::secret_matches(secret, &hash) {
            return Err(Error::forbidden("unknown token"));
        }
        let t = token_from_row(&row)?;
        if !t.active {
            return Err(Error::forbidden("token is revoked"));
        }
        Ok(t)
    }

    async fn notify_access(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        mapping_id: Option<&str>,
        role_id: Option<&str>,
    ) -> Result<()> {
        let payload =
            json!({"kind": "access", "mapping_id": mapping_id, "role_id": role_id}).to_string();
        sqlx::query("select pg_notify('skilj_changes', $1)")
            .bind(payload)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Commit an access change and publish it locally right away (the
    /// listener carries it to other instances).
    async fn commit_access(
        &self,
        tx: Transaction<'_, Postgres>,
        events: Vec<StoredEvent>,
        mapping_id: Option<String>,
        role_id: Option<String>,
    ) -> Result<()> {
        tx.commit().await?;
        self.after_commit(&events).await;
        let _ = self.inner.bus.send(super::BusMessage::AccessChanged {
            mapping_id,
            role_id,
        });
        Ok(())
    }

    fn require_superadmin(caller: &Role) -> Result<()> {
        if caller.active && caller.superadmin {
            Ok(())
        } else {
            Err(Error::forbidden("needs an active superadmin role"))
        }
    }

    // ------------------------------------------------------- bootstrap

    /// CreateSuperadmin.
    pub async fn create_superadmin(
        &self,
        bootstrap_secret: &str,
        name: &str,
        external_subject: &str,
    ) -> Result<Role> {
        let mut tx = self.inner.pool.begin().await?;
        sqlx::query("select pg_advisory_xact_lock(7243912)")
            .execute(&mut *tx)
            .await?;
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from roles where superadmin and active)")
                .fetch_one(&mut *tx)
                .await?;
        if exists {
            return Err(Error::forbidden(
                "a superadmin exists; the bootstrap is closed",
            ));
        }
        let ok = match self.inner.bootstrap_hash.lock().await.as_deref() {
            Some(h) => crypto::secret_matches(bootstrap_secret, h),
            None => false,
        };
        if !ok {
            return Err(Error::forbidden("wrong bootstrap secret"));
        }
        let role = self
            .insert_role(&mut tx, name, true, external_subject)
            .await?;
        tx.commit().await?;
        *self.inner.bootstrap_hash.lock().await = None;
        Ok(role)
    }

    async fn insert_role(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        name: &str,
        superadmin: bool,
        subject: &str,
    ) -> Result<Role> {
        if subject.is_empty() {
            return Err(Error::invalid("external_subject may not be empty"));
        }
        let taken: bool = sqlx::query_scalar(
            "select exists(select 1 from roles where external_subject = $1 and active)",
        )
        .bind(subject)
        .fetch_one(&mut **tx)
        .await?;
        if taken {
            return Err(Error::conflict(
                "that external identity is already bound to an active role",
            ));
        }
        let role = Role {
            id: crypto::generate_id(),
            external_subject: subject.into(),
            name: name.into(),
            superadmin,
            active: true,
            created_at: Self::now(),
            revoked_at: None,
        };
        sqlx::query("insert into roles (id, external_subject, name, superadmin, active, created_at) values ($1, $2, $3, $4, true, $5)")
            .bind(&role.id)
            .bind(&role.external_subject)
            .bind(&role.name)
            .bind(superadmin)
            .bind(role.created_at)
            .execute(&mut **tx)
            .await?;
        Ok(role)
    }

    /// CreateRole.
    pub async fn create_role(
        &self,
        caller: &Role,
        name: &str,
        superadmin: bool,
        external_subject: &str,
    ) -> Result<Role> {
        Self::require_superadmin(caller)?;
        let mut tx = self.inner.pool.begin().await?;
        let r = self
            .insert_role(&mut tx, name, superadmin, external_subject)
            .await?;
        tx.commit().await?;
        Ok(r)
    }

    pub async fn list_roles(&self, caller: &Role) -> Result<Vec<Role>> {
        Self::require_superadmin(caller)?;
        let rows = sqlx::query(&format!(
            "select {ROLE_COLUMNS} from roles order by created_at"
        ))
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter().map(role_from_row).collect()
    }

    /// RevokeRole: the Role and every active grant it holds.
    pub async fn revoke_role(&self, caller: &Role, role_id: &str) -> Result<()> {
        Self::require_superadmin(caller)?;
        let mut tx = self.inner.pool.begin().await?;
        let n = sqlx::query(
            "update roles set active = false, revoked_at = $2 where id = $1 and active",
        )
        .bind(role_id)
        .bind(Self::now())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(Error::conflict("role is not active"));
        }
        let mappings = sqlx::query(&format!(
            "update access_mappings set active = false, revoked_at = $2 where role_id = $1 and active returning {MAPPING_COLUMNS}"
        ))
        .bind(role_id)
        .bind(Self::now())
        .fetch_all(&mut *tx)
        .await?;
        let mut events = vec![];
        for m in mappings {
            let m = mapping_from_row(&m)?;
            events.extend(
                self.record_access_event(
                    &mut tx,
                    "AccessRevoked",
                    role_id,
                    &m.bounded_context,
                    json!({"actor_role_id": caller.id, "reason": "role_revoked"}),
                )
                .await?,
            );
        }
        self.notify_access(&mut tx, None, Some(role_id)).await?;
        self.commit_access(tx, events, None, Some(role_id.into()))
            .await?;
        // The bootstrap reopens only when no superadmin is left.
        Ok(())
    }

    /// GrantRoleAccessMapping.
    pub async fn grant_access(
        &self,
        caller: &Role,
        role_id: &str,
        bc: &str,
        level: AccessLevel,
        can_read_sensitive: bool,
        scope: Option<&str>,
    ) -> Result<AccessMapping> {
        Self::require_superadmin(caller)?;
        let role = self.role_by_id(role_id).await?;
        if !role.active {
            return Err(Error::conflict("role is revoked"));
        }
        self.inner.registry.read().await.is_active(bc)?;
        let mut tx = self.inner.pool.begin().await?;
        let (m, events) = self
            .insert_mapping(
                &mut tx,
                &caller.id,
                &role,
                bc,
                level,
                can_read_sensitive,
                scope,
            )
            .await?;
        self.notify_access(&mut tx, Some(&m.id), Some(role_id))
            .await?;
        self.commit_access(tx, events, Some(m.id.clone()), Some(role_id.into()))
            .await?;
        Ok(m)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn insert_mapping(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        actor: &str,
        role: &Role,
        bc: &str,
        level: AccessLevel,
        can_read_sensitive: bool,
        scope: Option<&str>,
    ) -> Result<(AccessMapping, Vec<StoredEvent>)> {
        let exists: bool = sqlx::query_scalar(
            "select exists(select 1 from access_mappings where role_id = $1 and bounded_context = $2 and active)",
        )
        .bind(&role.id)
        .bind(bc)
        .fetch_one(&mut **tx)
        .await?;
        if exists {
            return Err(Error::conflict(
                "the role already holds an active grant on this context",
            ));
        }
        let m = AccessMapping {
            id: crypto::generate_id(),
            role_id: role.id.clone(),
            bounded_context: bc.into(),
            level,
            can_read_sensitive,
            scope: scope.map(str::to_string),
            capabilities: BTreeSet::new(),
            delegable: BTreeSet::new(),
            active: true,
            created_at: Self::now(),
            revoked_at: None,
        };
        sqlx::query(
            "insert into access_mappings (id, role_id, bounded_context, level, can_read_sensitive, scope, active, created_at)
             values ($1, $2, $3, $4, $5, $6, true, $7)",
        )
        .bind(&m.id)
        .bind(&m.role_id)
        .bind(bc)
        .bind(level.as_str())
        .bind(can_read_sensitive)
        .bind(&m.scope)
        .bind(m.created_at)
        .execute(&mut **tx)
        .await?;
        let events = self
            .record_access_event(
                tx,
                "AccessGranted",
                &role.id,
                bc,
                json!({"actor_role_id": actor, "level": level.as_str(), "can_read_sensitive": can_read_sensitive,
                       "scope": scope, "capabilities": [], "delegable": []}),
            )
            .await?;
        Ok((m, events))
    }

    async fn mapping(&self, id: &str) -> Result<AccessMapping> {
        let row = sqlx::query(&format!(
            "select {MAPPING_COLUMNS} from access_mappings where id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| Error::not_found(format!("access mapping {id}")))?;
        mapping_from_row(&row)
    }

    /// Mappings of a context (superadmin view).
    pub async fn list_mappings(
        &self,
        caller: &Role,
        bc: Option<&str>,
    ) -> Result<Vec<AccessMapping>> {
        Self::require_superadmin(caller)?;
        let rows = sqlx::query(&format!(
            "select {MAPPING_COLUMNS} from access_mappings where ($1::text is null or bounded_context = $1) order by created_at"
        ))
        .bind(bc)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter().map(mapping_from_row).collect()
    }

    /// RevokeRoleAccessMapping.
    pub async fn revoke_access(&self, caller: &Role, mapping_id: &str) -> Result<()> {
        Self::require_superadmin(caller)?;
        let m = self.mapping(mapping_id).await?;
        let mut tx = self.inner.pool.begin().await?;
        let n = sqlx::query(
            "update access_mappings set active = false, revoked_at = $2 where id = $1 and active",
        )
        .bind(mapping_id)
        .bind(Self::now())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(Error::conflict("grant is not active"));
        }
        let events = self
            .record_access_event(
                &mut tx,
                "AccessRevoked",
                &m.role_id,
                &m.bounded_context,
                json!({"actor_role_id": caller.id, "reason": "grant_revoked"}),
            )
            .await?;
        self.notify_access(&mut tx, Some(mapping_id), None).await?;
        self.commit_access(tx, events, Some(mapping_id.into()), None)
            .await
    }

    async fn change_capability(
        &self,
        actor_role_id: &str,
        target_id: &str,
        capability: &str,
        add: bool,
        via: &str,
        delegable_check: Option<&Grant>,
    ) -> Result<()> {
        let target = self.mapping(target_id).await?;
        if !target.active {
            return Err(Error::conflict("target grant is not active"));
        }
        if let Some(admin) = delegable_check {
            if !admin.has_level(AccessLevel::Admin) {
                return Err(Error::forbidden("needs an admin grant"));
            }
            if target.bounded_context != admin.mapping.bounded_context {
                return Err(Error::not_found(format!("access mapping {target_id}")));
            }
            if target.role_id == admin.role.id {
                return Err(Error::forbidden(
                    "an admin cannot change its own capabilities",
                ));
            }
            if !admin.mapping.delegable.contains(capability) {
                return Err(Error::forbidden(format!(
                    "'{capability}' is not in this admin's delegable set"
                )));
            }
        }
        if add {
            validate::valid_capability_name(capability)?;
            if target.capabilities.contains(capability) {
                return Err(Error::conflict("grant already holds that capability"));
            }
            if target.capabilities.len() >= self.inner.config.max_capabilities_per_grant {
                return Err(Error::invalid("a grant holds at most 16 capabilities"));
            }
        } else if !target.capabilities.contains(capability) {
            return Err(Error::conflict("grant does not hold that capability"));
        }
        let mut tx = self.inner.pool.begin().await?;
        let sql = if add {
            "update access_mappings set capabilities = array_append(capabilities, $2) where id = $1 and active and not ($2 = any(capabilities))"
        } else {
            "update access_mappings set capabilities = array_remove(capabilities, $2) where id = $1 and active and $2 = any(capabilities)"
        };
        let n = sqlx::query(sql)
            .bind(target_id)
            .bind(capability)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            return Err(Error::conflict("grant changed concurrently"));
        }
        let events = self
            .record_access_event(
                &mut tx,
                if add {
                    "CapabilityGranted"
                } else {
                    "CapabilityRevoked"
                },
                &target.role_id,
                &target.bounded_context,
                json!({"actor_role_id": actor_role_id, "capability": capability, "via": via}),
            )
            .await?;
        self.notify_access(&mut tx, Some(target_id), None).await?;
        self.commit_access(tx, events, Some(target_id.into()), None)
            .await
    }

    pub async fn grant_capability_as_superadmin(
        &self,
        caller: &Role,
        mapping_id: &str,
        capability: &str,
    ) -> Result<()> {
        Self::require_superadmin(caller)?;
        self.change_capability(&caller.id, mapping_id, capability, true, "superadmin", None)
            .await
    }

    pub async fn revoke_capability_as_superadmin(
        &self,
        caller: &Role,
        mapping_id: &str,
        capability: &str,
    ) -> Result<()> {
        Self::require_superadmin(caller)?;
        self.change_capability(
            &caller.id,
            mapping_id,
            capability,
            false,
            "superadmin",
            None,
        )
        .await
    }

    pub async fn grant_capability_as_admin(
        &self,
        grant: &Grant,
        mapping_id: &str,
        capability: &str,
    ) -> Result<()> {
        self.change_capability(
            &grant.role.id,
            mapping_id,
            capability,
            true,
            "admin",
            Some(grant),
        )
        .await
    }

    pub async fn revoke_capability_as_admin(
        &self,
        grant: &Grant,
        mapping_id: &str,
        capability: &str,
    ) -> Result<()> {
        self.change_capability(
            &grant.role.id,
            mapping_id,
            capability,
            false,
            "admin",
            Some(grant),
        )
        .await
    }

    /// The grants of the admin's own context (surface CapabilityDelegation).
    pub async fn delegation_view(
        &self,
        grant: &Grant,
    ) -> Result<Vec<(String, String, AccessMapping)>> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("needs an admin grant"));
        }
        let rows = sqlx::query(&format!(
            "select r.name as role_name, {} from access_mappings m join roles r on r.id = m.role_id
             where m.bounded_context = $1 and m.active order by m.created_at",
            MAPPING_COLUMNS
                .split(", ")
                .map(|c| format!("m.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .bind(&grant.mapping.bounded_context)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let m = mapping_from_row(r)?;
                Ok((m.role_id.clone(), r.try_get::<String, _>("role_name")?, m))
            })
            .collect()
    }

    /// SetDelegableCapabilities.
    pub async fn set_delegable(
        &self,
        caller: &Role,
        mapping_id: &str,
        delegable: BTreeSet<String>,
    ) -> Result<()> {
        Self::require_superadmin(caller)?;
        for c in &delegable {
            validate::valid_capability_name(c)?;
        }
        if delegable.len() > self.inner.config.max_capabilities_per_grant {
            return Err(Error::invalid("at most 16 delegable capabilities"));
        }
        let target = self.mapping(mapping_id).await?;
        if !target.active {
            return Err(Error::conflict("target grant is not active"));
        }
        if target.level != AccessLevel::Admin {
            return Err(Error::invalid("only an admin grant delegates"));
        }
        let list: Vec<String> = delegable.iter().cloned().collect();
        let mut tx = self.inner.pool.begin().await?;
        sqlx::query("update access_mappings set delegable = $2 where id = $1")
            .bind(mapping_id)
            .bind(&list)
            .execute(&mut *tx)
            .await?;
        let events = self
            .record_access_event(
                &mut tx,
                "DelegableCapabilitiesSet",
                &target.role_id,
                &target.bounded_context,
                json!({"actor_role_id": caller.id, "delegable": list}),
            )
            .await?;
        self.notify_access(&mut tx, Some(mapping_id), None).await?;
        self.commit_access(tx, events, Some(mapping_id.into()), None)
            .await
    }

    // ---------------------------------------------------------- tokens

    /// The four minting rules, uniformly.
    #[allow(clippy::too_many_arguments)]
    pub async fn mint_token(
        &self,
        grant: &Grant,
        purpose: TokenPurpose,
        type_name: &str,
        scope: Option<&str>,
        start_from: Option<EventReadStartPosition>,
        start_at_sequence: Option<i64>,
        start_at_time: Option<DateTime<Utc>>,
    ) -> Result<MintedToken> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("minting a token needs an admin grant"));
        }
        let bc = &grant.mapping.bounded_context;
        {
            let reg = self.inner.registry.read().await;
            match purpose {
                TokenPurpose::Command => {
                    reg.command_type(bc, type_name)?;
                }
                _ => {
                    reg.event_type(bc, type_name)?;
                }
            }
        }
        // A scoped admin mints only within its own scope; omitted inherits.
        if let (Some(mine), Some(asked)) = (&grant.mapping.scope, scope) {
            if mine != asked {
                return Err(Error::forbidden(
                    "a scoped admin mints only tokens of its own scope",
                ));
            }
        }
        let minted_scope = scope
            .map(str::to_string)
            .or_else(|| grant.mapping.scope.clone());
        let start = start_from.unwrap_or_default();
        if purpose != TokenPurpose::EventRead
            && (start_from.is_some() || start_at_sequence.is_some() || start_at_time.is_some())
        {
            return Err(Error::invalid(
                "only an event read token takes a starting position",
            ));
        }
        match start {
            EventReadStartPosition::AtSequence
                if start_at_sequence.is_none() || start_at_time.is_some() =>
            {
                return Err(Error::invalid(
                    "at_sequence needs start_at_sequence and no start_at_time",
                ))
            }
            EventReadStartPosition::AtTime
                if start_at_time.is_none() || start_at_sequence.is_some() =>
            {
                return Err(Error::invalid(
                    "at_time needs start_at_time and no start_at_sequence",
                ))
            }
            EventReadStartPosition::Beginning | EventReadStartPosition::Latest
                if start_at_sequence.is_some() || start_at_time.is_some() =>
            {
                return Err(Error::invalid("beginning and latest take no position"))
            }
            _ => {}
        }
        let id = crypto::generate_id();
        let secret = crypto::generate_secret();
        let token = AccessToken {
            id: id.clone(),
            purpose,
            bounded_context: bc.clone(),
            type_name: type_name.into(),
            scope: minted_scope,
            active: true,
            created_at: Self::now(),
            revoked_at: None,
            start_from: start,
            start_at_sequence,
            start_at_time,
        };
        sqlx::query(
            "insert into tokens (id, secret_hash, purpose, bounded_context, type_name, scope, active, created_at, start_from, start_at_sequence, start_at_time)
             values ($1, $2, $3, $4, $5, $6, true, $7, $8, $9, $10)",
        )
        .bind(&id)
        .bind(crypto::hash_secret(&secret))
        .bind(purpose.as_str())
        .bind(bc)
        .bind(type_name)
        .bind(&token.scope)
        .bind(token.created_at)
        .bind(start.as_str())
        .bind(start_at_sequence)
        .bind(start_at_time)
        .execute(&self.inner.pool)
        .await?;
        Ok(MintedToken {
            token,
            bearer: format!("{id}.{secret}"),
        })
    }

    pub async fn list_tokens(&self, grant: &Grant) -> Result<Vec<AccessToken>> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("needs an admin grant"));
        }
        let rows = sqlx::query(&format!(
            "select {TOKEN_COLUMNS} from tokens where bounded_context = $1 and ($2::text is null or scope = $2) order by created_at"
        ))
        .bind(&grant.mapping.bounded_context)
        .bind(&grant.mapping.scope)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter().map(token_from_row).collect()
    }

    /// RevokeToken.
    pub async fn revoke_token(&self, grant: &Grant, token_id: &str) -> Result<()> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("revoking a token needs an admin grant"));
        }
        let row = sqlx::query(&format!("select {TOKEN_COLUMNS} from tokens where id = $1"))
            .bind(token_id)
            .fetch_optional(&self.inner.pool)
            .await?
            .ok_or_else(|| Error::not_found(format!("token {token_id}")))?;
        let t = token_from_row(&row)?;
        if t.bounded_context != grant.mapping.bounded_context {
            return Err(Error::not_found(format!("token {token_id}")));
        }
        if let Some(mine) = &grant.mapping.scope {
            if t.scope.as_deref() != Some(mine.as_str()) {
                return Err(Error::forbidden(
                    "a scoped admin revokes only tokens of its own scope",
                ));
            }
        }
        let n = sqlx::query(
            "update tokens set active = false, revoked_at = $2 where id = $1 and active",
        )
        .bind(token_id)
        .bind(Self::now())
        .execute(&self.inner.pool)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(Error::conflict("token is already revoked"));
        }
        Ok(())
    }

    // ------------------------------------------- private-field grants

    /// GrantPrivateFieldAccessForEvent / ForCommand (record: Left = event
    /// sequence, Right = command id; neither = blanket).
    pub async fn grant_private_field_access(
        &self,
        grant: &Grant,
        grantee_role_id: &str,
        event_sequence: Option<i64>,
        command_id: Option<&str>,
    ) -> Result<PrivateFieldGrant> {
        if !grant.has_level(AccessLevel::Read) {
            return Err(Error::forbidden("grant is revoked"));
        }
        if event_sequence.is_some() && command_id.is_some() {
            return Err(Error::invalid("a grant names one record at most"));
        }
        let grantee = self.role_by_id(grantee_role_id).await?;
        if !grantee.active {
            return Err(Error::conflict("grantee role is revoked"));
        }
        let bc = grant.mapping.bounded_context.clone();
        if let Some(seq) = event_sequence {
            let row = sqlx::query(&format!(
                "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence = $2"
            ))
            .bind(&bc)
            .bind(seq)
            .fetch_optional(&self.inner.pool)
            .await?
            .ok_or_else(|| Error::not_found(format!("event {bc}#{seq}")))?;
            let e = event_from_row(&row)?;
            let ty = self
                .inner
                .registry
                .read()
                .await
                .event_type(&bc, &e.event_type)?;
            if !is_default_private_reader(
                &ty.0.private_fields,
                Record::Event(&e),
                &grant.role.id,
                &grant.role.external_subject,
            ) {
                return Err(Error::forbidden(
                    "you are not this record's default private reader",
                ));
            }
        }
        if let Some(cid) = command_id {
            let row = sqlx::query(&format!("select {COMMAND_COLUMNS} from commands c where c.id = $1 and c.bounded_context = $2"))
                .bind(cid)
                .bind(&bc)
                .fetch_optional(&self.inner.pool)
                .await?
                .ok_or_else(|| Error::not_found(format!("command {cid}")))?;
            let c = command_from_row(&row)?;
            let ty = self
                .inner
                .registry
                .read()
                .await
                .command_type(&bc, &c.command_type)?;
            if !is_default_private_reader(
                &ty.0.private_fields,
                Record::Command(&c),
                &grant.role.id,
                &grant.role.external_subject,
            ) {
                return Err(Error::forbidden(
                    "you are not this record's default private reader",
                ));
            }
        }
        let g = PrivateFieldGrant {
            id: crypto::generate_id(),
            bounded_context: bc.clone(),
            grantor_role_id: grant.role.id.clone(),
            grantee_role_id: grantee.id.clone(),
            event_sequence,
            command_id: command_id.map(str::to_string),
            active: true,
            created_at: Self::now(),
            revoked_at: None,
        };
        let mut tx = self.inner.pool.begin().await?;
        sqlx::query(
            "insert into private_field_grants (id, bounded_context, grantor, grantee, event_sequence, command_id, active, created_at)
             values ($1, $2, $3, $4, $5, $6, true, $7)",
        )
        .bind(&g.id)
        .bind(&bc)
        .bind(&g.grantor_role_id)
        .bind(&g.grantee_role_id)
        .bind(g.event_sequence)
        .bind(&g.command_id)
        .bind(g.created_at)
        .execute(&mut *tx)
        .await?;
        let events = self
            .record_access_event(
                &mut tx,
                "PrivateFieldAccessGranted",
                &grantee.id,
                &bc,
                json!({"actor_role_id": grant.role.id, "grant_id": g.id, "event_sequence": event_sequence, "command_id": command_id}),
            )
            .await?;
        self.commit_access(tx, events, None, None).await?;
        Ok(g)
    }

    /// RevokePrivateFieldAccess: the grantor alone.
    pub async fn revoke_private_field_access(&self, grant: &Grant, grant_id: &str) -> Result<()> {
        if !grant.has_level(AccessLevel::Read) {
            return Err(Error::forbidden("grant is revoked"));
        }
        let row = sqlx::query(&format!("select {GRANT_COLUMNS} from private_field_grants where id = $1 and bounded_context = $2"))
            .bind(grant_id)
            .bind(&grant.mapping.bounded_context)
            .fetch_optional(&self.inner.pool)
            .await?
            .ok_or_else(|| Error::not_found(format!("grant {grant_id}")))?;
        let g = pfg_from_row(&row)?;
        if g.grantor_role_id != grant.role.id {
            return Err(Error::forbidden(
                "only the grantor revokes a private-field grant",
            ));
        }
        if !g.active {
            return Err(Error::conflict("grant is already revoked"));
        }
        let mut tx = self.inner.pool.begin().await?;
        sqlx::query(
            "update private_field_grants set active = false, revoked_at = $2 where id = $1",
        )
        .bind(grant_id)
        .bind(Self::now())
        .execute(&mut *tx)
        .await?;
        let events = self
            .record_access_event(
                &mut tx,
                "PrivateFieldAccessRevoked",
                &g.grantee_role_id,
                &g.bounded_context,
                json!({"actor_role_id": grant.role.id, "grant_id": grant_id}),
            )
            .await?;
        self.commit_access(tx, events, None, None).await
    }

    /// ListPrivateFieldGrants.
    pub async fn list_private_field_grants(
        &self,
        grant: &Grant,
        grantor: Option<&str>,
    ) -> Result<Vec<PrivateFieldGrant>> {
        if !grant.has_level(AccessLevel::Read) {
            return Err(Error::forbidden("grant is revoked"));
        }
        let subject = grantor.unwrap_or(&grant.role.id);
        if subject != grant.role.id && !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden(
                "listing another role's grants needs an admin grant",
            ));
        }
        let rows = sqlx::query(&format!(
            "select {GRANT_COLUMNS} from private_field_grants where bounded_context = $1 and grantor = $2 order by created_at"
        ))
        .bind(&grant.mapping.bounded_context)
        .bind(subject)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter().map(pfg_from_row).collect()
    }

    // ------------------------------------------------------- erasure

    /// Active encryption keys of the admin's context.
    pub async fn list_encryption_keys(&self, grant: &Grant) -> Result<Vec<EncryptionKeyInfo>> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("needs an admin grant"));
        }
        let rows = sqlx::query(
            "select id, bounded_context, subject_key, subject_value, status, created_at, destroyed_at
             from encryption_keys where bounded_context = $1 order by created_at",
        )
        .bind(&grant.mapping.bounded_context)
        .fetch_all(&self.inner.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(EncryptionKeyInfo {
                    id: r.try_get("id")?,
                    bounded_context: r.try_get("bounded_context")?,
                    subject_key: r.try_get("subject_key")?,
                    subject_value: r.try_get("subject_value")?,
                    active: r.try_get::<String, _>("status")? == "active",
                    created_at: r.try_get("created_at")?,
                    destroyed_at: r.try_get("destroyed_at")?,
                })
            })
            .collect()
    }

    /// ForgetSubject: destroy the key (and the wrapped key material with
    /// it), and drop the subject's blind tags where erasure is declared.
    pub async fn forget_subject(
        &self,
        grant: &Grant,
        subject_key: &str,
        subject_value: &str,
    ) -> Result<()> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden(
                "forgetting a subject needs an admin grant",
            ));
        }
        let bc = grant.mapping.bounded_context.clone();
        let mut tx = self.inner.pool.begin().await?;
        let id: Option<String> = sqlx::query_scalar(
            "update encryption_keys set status = 'destroyed', destroyed_at = $4, wrapped = null
             where bounded_context = $1 and subject_key = $2 and subject_value = $3 and status = 'active' returning id",
        )
        .bind(&bc)
        .bind(subject_key)
        .bind(subject_value)
        .bind(Self::now())
        .fetch_optional(&mut *tx)
        .await?;
        let id = id.ok_or_else(|| {
            Error::not_found(format!("active key for {subject_key}={subject_value}"))
        })?;
        let blinds = sqlx::query(
            "select tag_key, erasure_subject_tag from blind_tags where bounded_context = $1 and erasure_subject_key = $2",
        )
        .bind(&bc)
        .bind(subject_key)
        .fetch_all(&mut *tx)
        .await?;
        let mut touched = false;
        for b in blinds {
            let tag_key: String = b.try_get("tag_key")?;
            let subj_tag: Option<String> = b.try_get("erasure_subject_tag")?;
            let Some(subj_tag) = subj_tag else { continue };
            let filter = json!([{"key": subj_tag, "value": subject_value}]);
            sqlx::query(
                "update events set tags = (select coalesce(jsonb_agg(t), '[]'::jsonb) from jsonb_array_elements(tags) t where t->>'key' <> $3)
                 where bounded_context = $1 and tags @> $2",
            )
            .bind(&bc)
            .bind(&filter)
            .bind(&tag_key)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "delete from event_tags et using event_tags s
                 where et.bounded_context = $1 and et.key = $3 and s.bounded_context = et.bounded_context
                   and s.sequence = et.sequence and s.key = $2 and s.value = $4",
            )
            .bind(&bc)
            .bind(&subj_tag)
            .bind(&tag_key)
            .bind(subject_value)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "update commands set consistency_tags = (select coalesce(jsonb_agg(t), '[]'::jsonb) from jsonb_array_elements(consistency_tags) t where t->>'key' <> $3)
                 where bounded_context = $1 and consistency_tags @> $2",
            )
            .bind(&bc)
            .bind(&filter)
            .bind(&tag_key)
            .execute(&mut *tx)
            .await?;
            touched = true;
        }
        tx.commit().await?;
        self.forget_cached_key(&id).await;
        sqlx::query("select pg_notify('skilj_changes', $1)")
            .bind(
                json!({"kind": "key_destroyed", "id": id, "bc": bc, "invalidate_cache": touched})
                    .to_string(),
            )
            .execute(&self.inner.pool)
            .await?;
        if touched {
            self.inner.cache.invalidate(&bc);
            self.warm_context(&bc).await?;
        }
        Ok(())
    }

    // ----------------------------------------------------- the trail

    pub(crate) async fn register_access_event_types(&self) -> Result<()> {
        for name in ACCESS_EVENT_TYPES {
            let def = EventTypeDef {
                bounded_context: ADMIN_CONTEXT.into(),
                name: name.into(),
                schema: access_event_schema(name),
                schema_version: 1,
                tag_mappings: vec![
                    TagMapping::new("role", "role_id"),
                    TagMapping::new("bounded_context", "bounded_context"),
                ],
                owner_tag_key: None,
                sensitive_fields: vec![],
                private_fields: vec![],
                external_creation_allowed: false,
                direct_creation_allowed: false,
                system_triggered_allowed: false,
                system_triggered_schedule: None,
                missed_occurrence_policy: None,
                schedule_position: None,
                last_fired_at: None,
                event_read_allowed: false,
            };
            sqlx::query("insert into event_types (bounded_context, name, def) values ($1, $2, $3) on conflict do nothing")
                .bind(ADMIN_CONTEXT)
                .bind(name)
                .bind(serde_json::to_value(&def)?)
                .execute(&self.inner.pool)
                .await?;
        }
        Ok(())
    }

    /// fold_access_events(role, bounded_context, as_of).
    async fn fold_access(
        &self,
        role_id: &str,
        bc: &str,
        as_of: DateTime<Utc>,
    ) -> Result<(bool, AccessHistoryAnswer)> {
        let rows = sqlx::query(
            "select event_type, payload from events where bounded_context = 'admin'
               and payload->>'role_id' = $1 and payload->>'bounded_context' = $2
               and event_type = any($3) and created_at <= $4 order by sequence",
        )
        .bind(role_id)
        .bind(bc)
        .bind(
            ACCESS_EVENT_TYPES[..6]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        )
        .bind(as_of)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut a = AccessHistoryAnswer {
            known: !rows.is_empty(),
            held: false,
            level: None,
            capabilities: BTreeSet::new(),
            delegable: BTreeSet::new(),
        };
        let set = |v: &Value| -> BTreeSet<String> {
            v.as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        for r in rows {
            let t: String = r.try_get("event_type")?;
            let p: Value = r.try_get("payload")?;
            match t.as_str() {
                "AccessGranted" | "AccessBaselined" => {
                    a.held = true;
                    a.level = p["level"].as_str().and_then(AccessLevel::parse);
                    a.capabilities = set(&p["capabilities"]);
                    a.delegable = set(&p["delegable"]);
                }
                "AccessRevoked" => {
                    a.held = false;
                    a.level = None;
                    a.capabilities.clear();
                    a.delegable.clear();
                }
                "CapabilityGranted" => {
                    if let Some(c) = p["capability"].as_str() {
                        a.capabilities.insert(c.into());
                    }
                }
                "CapabilityRevoked" => {
                    if let Some(c) = p["capability"].as_str() {
                        a.capabilities.remove(c);
                    }
                }
                "DelegableCapabilitiesSet" => a.delegable = set(&p["delegable"]),
                _ => {}
            }
        }
        Ok((a.held, a))
    }

    /// BaselineAccessEvents.
    pub(crate) async fn baseline_access_events(&self) -> Result<()> {
        let rows = sqlx::query(&format!(
            "select {MAPPING_COLUMNS} from access_mappings where active"
        ))
        .fetch_all(&self.inner.pool)
        .await?;
        for r in rows {
            let m = mapping_from_row(&r)?;
            let (held, _) = self
                .fold_access(&m.role_id, &m.bounded_context, Self::now())
                .await?;
            if held {
                continue;
            }
            let mut tx = self.inner.pool.begin().await?;
            let events = self
                .record_access_event(
                    &mut tx,
                    "AccessBaselined",
                    &m.role_id,
                    &m.bounded_context,
                    json!({"actor_role_id": null, "level": m.level.as_str(), "can_read_sensitive": m.can_read_sensitive,
                           "scope": m.scope, "capabilities": m.capabilities, "delegable": m.delegable}),
                )
                .await?;
            tx.commit().await?;
            self.after_commit(&events).await;
        }
        Ok(())
    }

    /// CapabilityHistory.
    pub async fn capability_history(
        &self,
        caller: &Role,
        role_id: &str,
        bc: &str,
        as_of: DateTime<Utc>,
    ) -> Result<AccessHistoryAnswer> {
        Self::require_superadmin(caller)?;
        Ok(self.fold_access(role_id, bc, as_of).await?.1)
    }

    /// CapabilityOverview.
    pub async fn capability_overview(&self, caller: &Role) -> Result<Vec<CapabilityOverviewRow>> {
        Self::require_superadmin(caller)?;
        let reg = self.registry().await;
        let mut out = vec![];
        for (name, c) in &reg.contexts {
            if c.status != ContextStatus::Active {
                continue;
            }
            let mut required = BTreeSet::new();
            for p in reg.projections_of(name) {
                if let Some(cap) = &p.requires_capability {
                    required.insert(cap.clone());
                }
            }
            for t in reg.event_types_of(name) {
                for f in &t.0.private_fields {
                    if let Some(cap) = &f.capability {
                        required.insert(cap.clone());
                    }
                }
            }
            for t in reg.command_types_of(name) {
                for f in &t.0.private_fields {
                    if let Some(cap) = &f.capability {
                        required.insert(cap.clone());
                    }
                }
            }
            let held: BTreeSet<String> = sqlx::query_scalar::<_, String>(
                "select distinct unnest(capabilities) from access_mappings where bounded_context = $1 and active",
            )
            .bind(name)
            .fetch_all(&self.inner.pool)
            .await?
            .into_iter()
            .collect();
            out.push(CapabilityOverviewRow {
                bounded_context: name.clone(),
                required_not_held: required.difference(&held).cloned().collect(),
                held_not_required: held.difference(&required).cloned().collect(),
                required,
                held,
            });
        }
        Ok(out)
    }

    pub(crate) async fn mappings_by_role(&self) -> Result<BTreeMap<String, Vec<AccessMapping>>> {
        let rows = sqlx::query(&format!("select {MAPPING_COLUMNS} from access_mappings"))
            .fetch_all(&self.inner.pool)
            .await?;
        let mut out: BTreeMap<String, Vec<AccessMapping>> = BTreeMap::new();
        for r in rows {
            let m = mapping_from_row(&r)?;
            out.entry(m.bounded_context.clone()).or_default().push(m);
        }
        Ok(out)
    }
}

fn access_event_schema(name: &str) -> Value {
    let mut props = json!({
        "role_id": {"type": "string"},
        "bounded_context": {"type": "string"},
        "actor_role_id": {"type": ["string", "null"]}
    });
    let extra = match name {
        "AccessGranted" | "AccessBaselined" => json!({
            "level": {"type": "string"}, "can_read_sensitive": {"type": "boolean"},
            "scope": {"type": ["string", "null"]},
            "capabilities": {"type": "array", "items": {"type": "string"}},
            "delegable": {"type": "array", "items": {"type": "string"}}
        }),
        "AccessRevoked" => json!({"reason": {"type": "string"}}),
        "CapabilityGranted" | "CapabilityRevoked" => {
            json!({"capability": {"type": "string"}, "via": {"type": "string"}})
        }
        "DelegableCapabilitiesSet" => {
            json!({"delegable": {"type": "array", "items": {"type": "string"}}})
        }
        _ => json!({
            "grant_id": {"type": "string"},
            "event_sequence": {"type": ["integer", "null"]},
            "command_id": {"type": ["string", "null"]},
            "reason": {"type": "string"}
        }),
    };
    if let (Value::Object(p), Value::Object(e)) = (&mut props, extra) {
        p.extend(e);
    }
    json!({"type": "object", "properties": props, "required": ["role_id", "bounded_context"]})
}
