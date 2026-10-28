//! The per-bounded-context cache of the most recent events. Purely a read
//! optimisation: a read it can't fully answer goes to Postgres.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::model::StoredEvent;

#[derive(Default)]
pub(crate) struct EventCache {
    rings: Mutex<HashMap<String, VecDeque<Arc<StoredEvent>>>>,
}

impl EventCache {
    pub(crate) fn warm(&self, bc: &str, events: Vec<StoredEvent>, cap: usize) {
        let mut ring: VecDeque<Arc<StoredEvent>> = events.into_iter().map(Arc::new).collect();
        while ring.len() > cap {
            ring.pop_front();
        }
        self.rings.lock().unwrap().insert(bc.to_string(), ring);
    }

    /// Append a committed event. Out-of-order or gapped appends drop the
    /// ring rather than leave a hole in it.
    pub(crate) fn append(&self, e: Arc<StoredEvent>, cap: usize) {
        let mut rings = self.rings.lock().unwrap();
        let ring = rings.entry(e.bounded_context.clone()).or_default();
        match ring.back().map(|l| l.sequence) {
            Some(last) if e.sequence <= last => return,
            Some(last) if e.sequence != last + 1 => ring.clear(),
            None if e.sequence != 0 => {
                // An empty ring for a context with history: we don't know
                // what came before, so keep it empty until re-warmed.
                return;
            }
            _ => {}
        }
        ring.push_back(e);
        while ring.len() > cap {
            ring.pop_front();
        }
    }

    pub(crate) fn invalidate(&self, bc: &str) {
        self.rings.lock().unwrap().remove(bc);
    }

    /// Events with `after < sequence <= upto`, when the cache holds that
    /// whole span.
    pub(crate) fn span(&self, bc: &str, after: i64, upto: i64) -> Option<Vec<Arc<StoredEvent>>> {
        let rings = self.rings.lock().unwrap();
        let ring = rings.get(bc)?;
        if upto <= after {
            return Some(vec![]);
        }
        let first = ring.front()?.sequence;
        let last = ring.back()?.sequence;
        if first > after + 1 || last < upto {
            return None;
        }
        Some(
            ring.iter()
                .filter(|e| e.sequence > after && e.sequence <= upto)
                .cloned()
                .collect(),
        )
    }
}
