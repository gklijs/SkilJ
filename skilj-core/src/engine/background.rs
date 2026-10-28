//! Background work: the cross-instance listener, the async projection
//! consumer, the scheduler and housekeeping.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use croner::Cron;
use serde_json::Value;
use sqlx::postgres::PgListener;

use super::{BusMessage, Skilj};
use crate::error::{Error, Result};
use crate::model::*;

pub(crate) fn parse_cron(expr: &str) -> Result<Cron> {
    Cron::new(expr)
        .parse()
        .map_err(|e| Error::invalid(format!("bad cron expression '{expr}': {e}")))
}

/// next_occurrence_after(schedule, instant): strictly after.
pub(crate) fn next_occurrence_after(cron: &Cron, t: DateTime<Utc>) -> Result<DateTime<Utc>> {
    cron.find_next_occurrence(&t, false)
        .map_err(|e| Error::invalid(format!("cron: {e}")))
}

pub(crate) fn spawn(skilj: Skilj) {
    let s = skilj.clone();
    tokio::spawn(async move { s.listen_forever().await });
    let s = skilj.clone();
    tokio::spawn(async move {
        // Skip-policy types resume at startup.
        if let Err(e) = s.resume_schedules().await {
            tracing::warn!("resume schedules: {e}");
        }
        let mut housekeeping = tokio::time::Instant::now();
        loop {
            if let Err(e) = s.tick().await {
                tracing::warn!("background tick: {e}");
            }
            if housekeeping.elapsed() > std::time::Duration::from_secs(60) {
                if let Err(e) = s.housekeeping().await {
                    tracing::warn!("housekeeping: {e}");
                }
                housekeeping = tokio::time::Instant::now();
            }
            tokio::time::sleep(s.inner.config.background_tick).await;
        }
    });
}

impl Skilj {
    /// One round of background work (tests call this directly).
    pub async fn tick(&self) -> Result<()> {
        while self.step_projections().await? > 0 {}
        self.step_schedules().await?;
        Ok(())
    }

    /// Expire idempotency records past the retention.
    pub async fn housekeeping(&self) -> Result<()> {
        let cutoff = Self::now() - self.inner.config.idempotency_key_retention;
        sqlx::query("delete from idempotency_keys where recorded_at < $1")
            .bind(cutoff)
            .execute(&self.inner.pool)
            .await?;
        sqlx::query("delete from external_keys where recorded_at < $1")
            .bind(cutoff)
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    async fn listen_forever(&self) {
        loop {
            match self.listen().await {
                Ok(()) => {}
                Err(e) => tracing::warn!("listener dropped: {e}"),
            }
            // We may have missed notifications: live subscriptions end so
            // their callers resume from what they received, and state is
            // re-read from the database.
            let _ = self.inner.bus.send(BusMessage::Lagged);
            let _ = self.reload_registry().await;
            let _ = self.warm_cache().await;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    async fn listen(&self) -> Result<()> {
        let mut l = PgListener::connect_with(&self.inner.pool).await?;
        l.listen_all(["skilj_events", "skilj_changes"]).await?;
        // Catch up on anything committed before we were listening.
        self.reload_registry().await?;
        self.warm_cache().await?;
        loop {
            let n = l.recv().await?;
            let v: Value = serde_json::from_str(n.payload()).unwrap_or(Value::Null);
            match n.channel() {
                "skilj_events" => {
                    if let (Some(bc), Some(from), Some(to)) =
                        (v["bc"].as_str(), v["from"].as_i64(), v["to"].as_i64())
                    {
                        self.dispatch_range(bc, from, to).await?;
                    }
                }
                _ => match v["kind"].as_str() {
                    Some("registry") => self.reload_registry().await?,
                    Some("access") => {
                        let _ = self.inner.bus.send(BusMessage::AccessChanged {
                            mapping_id: v["mapping_id"].as_str().map(str::to_string),
                            role_id: v["role_id"].as_str().map(str::to_string),
                        });
                    }
                    Some("key_destroyed") => {
                        if let Some(id) = v["id"].as_str() {
                            self.forget_cached_key(id).await;
                        }
                        if v["invalidate_cache"].as_bool() == Some(true) {
                            if let Some(bc) = v["bc"].as_str() {
                                self.inner.cache.invalidate(bc);
                                self.warm_context(bc).await?;
                            }
                        }
                    }
                    _ => {}
                },
            }
        }
    }

    /// Scheduled event types this process can produce payloads for.
    async fn scheduled_types(&self) -> Vec<(String, String)> {
        let reg = self.inner.registry.read().await;
        reg.event_types
            .iter()
            .filter(|(_, t)| t.0.system_triggered_allowed)
            .filter(|((bc, _), _)| {
                reg.contexts
                    .get(bc)
                    .is_some_and(|c| c.status == ContextStatus::Active)
            })
            .filter(|((bc, name), _)| self.scheduled_payload_fn(bc, name).is_some())
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn scheduled_payload_fn(
        &self,
        bc: &str,
        name: &str,
    ) -> Option<crate::decl::ScheduledPayloadFn> {
        self.decl_for(bc)?
            .event_types
            .iter()
            .find(|e| e.name == name)
            .and_then(|e| e.schedule.as_ref().map(|s| s.2.clone()))
    }

    /// SkipMissedOccurrences for every skip-policy type: this instance is
    /// becoming responsible for its schedules.
    pub async fn resume_schedules(&self) -> Result<()> {
        for (bc, name) in self.scheduled_types().await {
            let mut tx = self.inner.pool.begin().await?;
            let def: Option<Value> = sqlx::query_scalar(
                "select def from event_types where bounded_context = $1 and name = $2 for update",
            )
            .bind(&bc)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(def) = def else { continue };
            let mut def: EventTypeDef = serde_json::from_value(def)?;
            let now = Self::now();
            if def.missed_occurrence_policy == Some(MissedOccurrencePolicy::Skip)
                && def.schedule_position.is_some_and(|p| p < now)
            {
                def.schedule_position = Some(now);
                sqlx::query(
                    "update event_types set def = $3 where bounded_context = $1 and name = $2",
                )
                .bind(&bc)
                .bind(&name)
                .bind(serde_json::to_value(&def)?)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }

    /// CreateSystemEvent for every due occurrence, per policy.
    pub async fn step_schedules(&self) -> Result<usize> {
        let mut fired = 0;
        for (bc, name) in self.scheduled_types().await {
            let Some(payload_fn) = self.scheduled_payload_fn(&bc, &name) else {
                continue;
            };
            let mut tx = self.inner.pool.begin().await?;
            let def: Option<Value> = sqlx::query_scalar(
                "select def from event_types where bounded_context = $1 and name = $2 for update skip locked",
            )
            .bind(&bc)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(def) = def else { continue };
            let mut def: EventTypeDef = serde_json::from_value(def)?;
            let (Some(expr), Some(policy), Some(mut pos)) = (
                def.system_triggered_schedule.clone(),
                def.missed_occurrence_policy,
                def.schedule_position,
            ) else {
                continue;
            };
            let cron = parse_cron(&expr)?;
            let now = Self::now();
            let mut written = vec![];
            for _ in 0..100 {
                let occ = next_occurrence_after(&cron, pos)?;
                if occ > now {
                    break;
                }
                let nothing_later_due = next_occurrence_after(&cron, occ)? > now;
                let fire = match policy {
                    MissedOccurrencePolicy::ReplayBacklog => true,
                    MissedOccurrencePolicy::FireOnce => nothing_later_due,
                    // no_gap holds by construction (occ follows pos).
                    MissedOccurrencePolicy::Skip => nothing_later_due,
                };
                if fire {
                    written.extend(
                        self.create_system_event(&mut tx, &def, payload_fn(occ), occ)
                            .await?,
                    );
                    def.last_fired_at = Some(occ);
                    pos = occ;
                    fired += 1;
                } else if policy == MissedOccurrencePolicy::FireOnce {
                    // Passed over: accounted for when the last one fires.
                    pos = occ;
                } else {
                    // skip with a backlog: a gap nobody resumed. Close it the
                    // way SkipMissedOccurrences does.
                    pos = now;
                    break;
                }
            }
            if Some(pos) != def.schedule_position {
                def.schedule_position = Some(pos);
                sqlx::query(
                    "update event_types set def = $3 where bounded_context = $1 and name = $2",
                )
                .bind(&bc)
                .bind(&name)
                .bind(serde_json::to_value(&def)?)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            self.after_commit(&written).await;
            if !written.is_empty() {
                self.reload_registry().await?;
            }
        }
        Ok(fired)
    }
}

#[allow(dead_code)]
fn _cron_check() -> Result<()> {
    let _ = Cron::from_str("* * * * *");
    Ok(())
}
