//! Settles a legacy duplicate during the worker-free schema upgrade.

use skilj_core::access_control::{self, AccessLevel, IdpConfig, JwksCache, SigningAlgorithm};
use skilj_core::approvals::{CommandProposalStatus, Error as ApprovalError};
use skilj_core::{db, Error};
use std::io::{self, Read};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityConfig {
    jwks_endpoint: String,
    issuer: String,
    audiences: Vec<String>,
    signing_algorithm: Algorithm,
    #[serde(default)]
    subject_claim: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum Algorithm {
    Rs256,
    Rs384,
    Rs512,
    Es256,
    Es384,
}

impl IdentityConfig {
    fn into_idp(self) -> Result<IdpConfig, &'static str> {
        let mut audiences = self.audiences.into_iter();
        let first = audiences
            .next()
            .ok_or("at least one IdP audience is required")?;
        let algorithm = match self.signing_algorithm {
            Algorithm::Rs256 => SigningAlgorithm::Rs256,
            Algorithm::Rs384 => SigningAlgorithm::Rs384,
            Algorithm::Rs512 => SigningAlgorithm::Rs512,
            Algorithm::Es256 => SigningAlgorithm::Es256,
            Algorithm::Es384 => SigningAlgorithm::Es384,
        };
        let endpoint = self
            .jwks_endpoint
            .parse()
            .map_err(|_| "invalid JWKS endpoint")?;
        let mut config = IdpConfig::new(endpoint, self.issuer, first, algorithm);
        for audience in audiences {
            config = config.with_additional_audience(audience);
        }
        if let Some(claim) = self.subject_claim {
            config = config.with_subject_claim(claim);
        }
        Ok(config)
    }
}

pub(crate) async fn settle(
    pool: &db::Pool,
    config: &IdpConfig,
    jwt: &str,
    bounded_context: &str,
    proposal_id: &str,
    reason: &str,
) -> Result<CommandProposalStatus, Error> {
    let cache = JwksCache::new(config.jwks_endpoint.clone());
    let verified = access_control::verify_jwt(jwt, config, &cache).await?;
    let roles = db::active_roles_by_external_subject(pool, &verified.subject).await?;
    let role = access_control::resolve_role_by_external_subject(&verified.subject, &roles)?;
    let mapping = db::get_active_role_access_mapping(pool, &role.id, bounded_context)
        .await?
        .filter(|mapping| mapping.level == AccessLevel::Admin)
        .ok_or(access_control::Error::GrantNotActive)?;
    match db::reject_command_proposal(
        pool,
        bounded_context,
        proposal_id,
        &mapping,
        Some(reason),
        jiff::Timestamp::now(),
    )
    .await
    {
        Ok(proposal) => Ok(proposal.status),
        // These decisions already passed the same scope checks as a
        // successful rejection. In particular, do not perform a raw
        // proposal lookup to guess the status after some other error.
        Err(Error::Approvals(ApprovalError::ProposalNotPending { status: "approved" })) => {
            Ok(CommandProposalStatus::Approved)
        }
        Err(Error::Approvals(ApprovalError::ProposalExpired)) => Ok(CommandProposalStatus::Expired),
        Err(err) => Err(err),
    }
}

fn arguments(args: impl Iterator<Item = String>) -> Result<[String; 3], &'static str> {
    args.collect::<Vec<_>>()
        .try_into()
        .map_err(|_| "usage: reject_duplicate_proposal <bounded-context> <proposal-id> <reason>")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let [bounded_context, proposal_id, reason] = arguments(std::env::args().skip(1))?;
    let settings_path = std::env::var("SKILJ_RECOVERY_IDP_CONFIG")?;
    let settings: IdentityConfig = serde_json::from_slice(&std::fs::read(settings_path)?)?;
    let config = settings.into_idp()?;
    let mut jwt = String::new();
    io::stdin().read_to_string(&mut jwt)?;
    let pool = db::connect(&std::env::var("DATABASE_URL")?).await?;
    let status = settle(
        &pool,
        &config,
        jwt.trim(),
        &bounded_context,
        &proposal_id,
        &reason,
    )
    .await?;
    println!("{proposal_id}: {}", status.as_str());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_allow_no_actor_override() {
        assert_eq!(
            arguments(
                ["context", "proposal", "duplicate"]
                    .map(str::to_string)
                    .into_iter()
            )
            .unwrap(),
            ["context", "proposal", "duplicate"],
        );
        assert!(arguments(
            ["context", "proposal", "duplicate", "some-admin"]
                .map(str::to_string)
                .into_iter()
        )
        .is_err());
    }
}
