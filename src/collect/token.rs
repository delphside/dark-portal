//! The access token and its cache: `state/blizzard-token.json` under
//! `DP_DATA_DIR`, mode 0600, written to a temporary name and renamed so a
//! reader sees only a whole file.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::failure::{Failure, io_failure};
use crate::config::Secret;

/// The cache's place under the data folder.
const CACHE_FILE: &str = "state/blizzard-token.json";

/// An access token and when it expires. Its `Debug` never prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    value: Secret,
    expires_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct Stored {
    access_token: String,
    expires_at: i64,
}

impl Token {
    /// A token that expires `lifetime` after `now`.
    pub fn new(value: Secret, now: DateTime<Utc>, lifetime: Duration) -> Token {
        Token {
            value,
            expires_at: now + lifetime,
        }
    }

    /// The token itself, for the one header that sends it.
    pub fn expose(&self) -> &str {
        self.value.expose()
    }

    /// Whether more than `margin` is left of it at `now`.
    pub fn has_more_than(&self, margin: Duration, now: DateTime<Utc>) -> bool {
        self.expires_at - now > chrono::Duration::from_std(margin).unwrap_or(chrono::Duration::MAX)
    }
}

/// The token's file.
#[derive(Debug, Clone)]
pub struct TokenCache {
    path: PathBuf,
}

impl TokenCache {
    /// The cache under `data_dir`.
    pub fn new(data_dir: &Path) -> TokenCache {
        TokenCache {
            path: data_dir.join(CACHE_FILE),
        }
    }

    /// The cached token, or `None` where there is none or it cannot be read:
    /// a damaged cache is a missing one, and the run takes a new token.
    pub fn load(&self) -> Option<Token> {
        let text = fs::read_to_string(&self.path).ok()?;
        let stored: Stored = serde_json::from_str(&text).ok()?;
        Some(Token {
            value: Secret::new(stored.access_token),
            expires_at: DateTime::from_timestamp(stored.expires_at, 0)?,
        })
    }

    /// Writes the token: to a temporary name with mode 0600, then renamed.
    pub fn store(&self, token: &Token) -> Result<(), Failure> {
        let stored = Stored {
            access_token: token.value.expose().to_owned(),
            expires_at: token.expires_at.timestamp(),
        };
        let text = serde_json::to_string(&stored).expect("a token serialises");
        let result = (|| {
            if let Some(dir) = self.path.parent() {
                fs::create_dir_all(dir)?;
            }
            let temporary = self.path.with_extension("json.tmp");
            let _ = fs::remove_file(&temporary);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)
        })();
        result.map_err(|e| io_failure(&e, "writing the token cache"))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    const VALUE: &str = "tok-abc-123";

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("dp-token-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    fn token() -> Token {
        Token::new(
            Secret::new(VALUE),
            at("2026-11-26T10:00:00Z"),
            Duration::from_secs(24 * 3600),
        )
    }

    #[test]
    fn a_stored_token_is_loaded_back() {
        let cache = TokenCache::new(&dir("roundtrip"));
        cache.store(&token()).unwrap();
        assert_eq!(cache.load().unwrap(), token());
    }

    #[test]
    fn the_cache_is_mode_0600_and_leaves_no_temporary_file() {
        let root = dir("mode");
        let cache = TokenCache::new(&root);
        cache.store(&token()).unwrap();
        let mode = fs::metadata(root.join(CACHE_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let names: Vec<_> = fs::read_dir(root.join("state"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec!["blizzard-token.json"]);
    }

    #[test]
    fn a_stale_temporary_file_does_not_stop_a_store() {
        let root = dir("stale");
        fs::create_dir_all(root.join("state")).unwrap();
        fs::write(root.join("state/blizzard-token.json.tmp"), "junk").unwrap();
        let cache = TokenCache::new(&root);
        cache.store(&token()).unwrap();
        assert_eq!(cache.load().unwrap(), token());
    }

    #[test]
    fn a_missing_or_damaged_cache_loads_as_none() {
        let root = dir("damaged");
        let cache = TokenCache::new(&root);
        assert!(cache.load().is_none());
        fs::create_dir_all(root.join("state")).unwrap();
        fs::write(root.join(CACHE_FILE), "{not json").unwrap();
        assert!(cache.load().is_none());
    }

    #[test]
    fn a_token_is_reused_only_while_more_than_the_margin_is_left() {
        let margin = Duration::from_secs(3600);
        let token = token(); // expires 2026-11-27T10:00
        assert!(token.has_more_than(margin, at("2026-11-27T08:59:59Z")));
        assert!(!token.has_more_than(margin, at("2026-11-27T09:00:00Z")));
        assert!(!token.has_more_than(margin, at("2026-11-27T11:00:00Z")));
    }

    #[test]
    fn the_token_is_never_printed() {
        let text = format!("{:?} {:#?}", token(), token());
        assert!(!text.contains(VALUE));
    }
}
