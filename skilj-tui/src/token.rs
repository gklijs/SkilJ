//! Where the bearer JWT comes from. This crate never talks to an IdP
//! itself, so a token either is given once (`--token`) or comes from a
//! command the operator configures (`--token-command`), which is run
//! again whenever the server says the current one has expired - a 4403
//! websocket close, or a query refused with `jwt_verification_failed`
//! (docs/architecture.md §137).

use crate::graphql::ClientError;
use std::sync::Arc;
use std::time::Duration;

/// How long a token command may run. Long enough for a helper that
/// opens a browser for a login.
pub const TOKEN_COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

/// The current bearer token, shared by every request and subscription.
/// Cloning shares it.
#[derive(Clone)]
pub struct TokenSource {
    command: Option<Arc<str>>,
    current: Arc<tokio::sync::Mutex<String>>,
}

impl From<String> for TokenSource {
    fn from(token: String) -> Self {
        Self::fixed(token)
    }
}

impl TokenSource {
    /// A token that is never replaced.
    pub fn fixed(token: String) -> Self {
        Self {
            command: None,
            current: Arc::new(tokio::sync::Mutex::new(token)),
        }
    }

    /// Runs `command` (through the platform shell) for the first token,
    /// and again on every [`refresh`](Self::refresh). Its trimmed stdout
    /// is the token.
    pub async fn from_command(command: String) -> Result<Self, ClientError> {
        let token = run_token_command(&command).await?;
        Ok(Self {
            command: Some(command.into()),
            current: Arc::new(tokio::sync::Mutex::new(token)),
        })
    }

    pub async fn current(&self) -> String {
        self.current.lock().await.clone()
    }

    /// Whether [`refresh`](Self::refresh) can produce a different token.
    pub fn can_refresh(&self) -> bool {
        self.command.is_some()
    }

    /// Replaces `stale` - the token a caller just had refused - with a
    /// fresh one from the command. When another caller has already
    /// replaced it, that token is returned without running the command
    /// again, so several requests failing at once run it once.
    /// `Ok(None)` for a fixed token: there is nothing to refresh it with.
    pub async fn refresh(&self, stale: &str) -> Result<Option<String>, ClientError> {
        let Some(command) = &self.command else {
            return Ok(None);
        };
        let mut current = self.current.lock().await;
        if *current != stale {
            return Ok(Some(current.clone()));
        }
        let fresh = run_token_command(command).await?;
        *current = fresh.clone();
        Ok(Some(fresh))
    }
}

async fn run_token_command(command: &str) -> Result<String, ClientError> {
    #[cfg(windows)]
    let mut process = {
        let mut process = tokio::process::Command::new("cmd");
        process.arg("/C").arg(command);
        process
    };
    #[cfg(not(windows))]
    let mut process = {
        let mut process = tokio::process::Command::new("sh");
        process.arg("-c").arg(command);
        process
    };
    // stdin/stderr are kept off the terminal: the TUI owns it (raw
    // mode, alternate screen) once running.
    process
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let output = tokio::time::timeout(TOKEN_COMMAND_TIMEOUT, process.output())
        .await
        .map_err(|_| {
            ClientError::TokenCommand(format!(
                "timed out after {}s",
                TOKEN_COMMAND_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|e| ClientError::TokenCommand(format!("could not run it: {e}")))?;
    if !output.status.success() {
        return Err(ClientError::TokenCommand(format!(
            "{}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let token = String::from_utf8(output.stdout)
        .map_err(|_| ClientError::TokenCommand("its output is not UTF-8".to_string()))?
        .trim()
        .to_string();
    if token.is_empty() {
        return Err(ClientError::TokenCommand("it printed no token".to_string()));
    }
    Ok(token)
}
