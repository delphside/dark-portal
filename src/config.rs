//! Deployment settings, read from the environment variables listed in
//! [5.4.1](../docs/5.4.1-configuration.md#deployment-settings). A missing or
//! malformed one is a *technical, fundamental* error: the operator fixes the
//! environment's `.env`.

use std::fmt;
use std::path::PathBuf;

use crate::error::{ErrorType, Report};

/// A secret: its `Debug` and `Display` never print it, so it cannot reach a
/// log by accident. It is never logged, nor written to bronze or the export
/// ([5.4.1](../docs/5.4.1-configuration.md#deployment-settings)).
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Holds a secret.
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(value.into())
    }

    /// The secret itself, for the one call that sends it to Blizzard.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// The deployment settings of one environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// `DP_ENV`: the environment's name.
    pub env: String,
    /// `DP_DATA_DIR`: the environment's data folder.
    pub data_dir: PathBuf,
    /// `DP_BLIZZARD_CLIENT_ID`: the Blizzard client's ID.
    pub blizzard_client_id: String,
    /// `DP_BLIZZARD_CLIENT_SECRET_FILE`'s contents: the client's secret.
    pub blizzard_client_secret: Secret,
    /// `DP_BUCKET`: the Object Storage bucket, or `None` for none.
    pub bucket: Option<String>,
    /// `DP_LOCK_FILE`: the VM-wide lock every writing job takes.
    pub lock_file: PathBuf,
    /// `DP_ADHOC_MEMORY_LIMIT`: DuckDB's `memory_limit` for an ad hoc query.
    pub adhoc_memory_limit: String,
    /// `DP_ADHOC_THREADS`: DuckDB's `threads` for an ad hoc query.
    pub adhoc_threads: u32,
    /// `DP_CONTAINER_MEMORY`: the container's memory limit.
    pub container_memory: String,
}

fn fundamental(reason: &str, message: &str, variable: &str) -> Box<Report> {
    Box::new(
        Report::fatal(ErrorType::TechnicalFundamental, reason, message)
            .with_metadata("variable", variable),
    )
}

impl Config {
    /// Reads the settings from the process's environment.
    pub fn from_env() -> Result<Config, Box<Report>> {
        Config::from_lookup(|name| std::env::var(name).ok())
    }

    /// Reads the settings through `lookup`, which gives a variable's value or
    /// `None`; [`from_env`](Config::from_env) passes the environment, a test
    /// passes a map. Reads the secret from the file `DP_BLIZZARD_CLIENT_SECRET_FILE`
    /// names, without its trailing newline.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Config, Box<Report>> {
        let required = |name: &str| {
            lookup(name).filter(|v| !v.is_empty()).ok_or_else(|| {
                fundamental(
                    "MISSING_SETTING",
                    "A deployment setting is missing; set it in the environment's .env.",
                    name,
                )
            })
        };

        let secret_file = required("DP_BLIZZARD_CLIENT_SECRET_FILE")?;
        let secret = std::fs::read_to_string(&secret_file).map_err(|e| {
            let mut report = fundamental(
                "SECRET_FILE_UNREADABLE",
                "The Blizzard client secret file cannot be read; check its path and its owner.",
                "DP_BLIZZARD_CLIENT_SECRET_FILE",
            );
            report
                .metadata
                .insert("kind".to_owned(), format!("{:?}", e.kind()));
            report
        })?;
        let secret = secret.trim_end_matches(['\n', '\r']);
        if secret.is_empty() {
            return Err(fundamental(
                "SECRET_FILE_EMPTY",
                "The Blizzard client secret file is empty; put the secret in it.",
                "DP_BLIZZARD_CLIENT_SECRET_FILE",
            ));
        }

        let threads = required("DP_ADHOC_THREADS")?;
        let adhoc_threads = threads.parse::<u32>().ok().filter(|n| *n > 0).ok_or_else(|| {
            fundamental(
                "MALFORMED_SETTING",
                "A deployment setting is not in the expected form; correct it in the environment's .env.",
                "DP_ADHOC_THREADS",
            )
        })?;

        Ok(Config {
            env: required("DP_ENV")?,
            data_dir: PathBuf::from(required("DP_DATA_DIR")?),
            blizzard_client_id: required("DP_BLIZZARD_CLIENT_ID")?,
            blizzard_client_secret: Secret::new(secret),
            bucket: lookup("DP_BUCKET").filter(|v| !v.is_empty()),
            lock_file: PathBuf::from(required("DP_LOCK_FILE")?),
            adhoc_memory_limit: required("DP_ADHOC_MEMORY_LIMIT")?,
            adhoc_threads,
            container_memory: required("DP_CONTAINER_MEMORY")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::*;
    use crate::error::{ExitStatus, exit_status};

    const SECRET: &str = "s3cr3t-value";

    fn secret_file(dir: &Path) -> PathBuf {
        let path = dir.join("secret");
        std::fs::write(&path, format!("{SECRET}\n")).unwrap();
        path
    }

    fn vars(secret_path: &Path) -> HashMap<String, String> {
        [
            ("DP_ENV", "production"),
            ("DP_DATA_DIR", "/srv/dp/production"),
            ("DP_BLIZZARD_CLIENT_ID", "client-id"),
            (
                "DP_BLIZZARD_CLIENT_SECRET_FILE",
                secret_path.to_str().unwrap(),
            ),
            ("DP_BUCKET", "bucket"),
            ("DP_LOCK_FILE", "/srv/dp/dp.lock"),
            ("DP_ADHOC_MEMORY_LIMIT", "4GB"),
            ("DP_ADHOC_THREADS", "1"),
            ("DP_CONTAINER_MEMORY", "5GB"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    fn read(vars: &HashMap<String, String>) -> Result<Config, Box<Report>> {
        Config::from_lookup(|name| vars.get(name).cloned())
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("dp-config-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn every_setting_is_read() {
        let dir = temp_dir("read");
        let config = read(&vars(&secret_file(&dir))).unwrap();
        assert_eq!(config.env, "production");
        assert_eq!(config.data_dir, PathBuf::from("/srv/dp/production"));
        assert_eq!(config.blizzard_client_id, "client-id");
        assert_eq!(config.blizzard_client_secret.expose(), SECRET);
        assert_eq!(config.bucket.as_deref(), Some("bucket"));
        assert_eq!(config.lock_file, PathBuf::from("/srv/dp/dp.lock"));
        assert_eq!(config.adhoc_memory_limit, "4GB");
        assert_eq!(config.adhoc_threads, 1);
        assert_eq!(config.container_memory, "5GB");
    }

    #[test]
    fn an_empty_or_absent_bucket_is_none() {
        let dir = temp_dir("bucket");
        let mut v = vars(&secret_file(&dir));
        v.insert("DP_BUCKET".into(), String::new());
        assert_eq!(read(&v).unwrap().bucket, None);
        v.remove("DP_BUCKET");
        assert_eq!(read(&v).unwrap().bucket, None);
    }

    #[test]
    fn the_secret_is_never_printed() {
        let dir = temp_dir("print");
        let config = read(&vars(&secret_file(&dir))).unwrap();
        assert!(!format!("{config:?}").contains(SECRET));
        assert!(!format!("{config:#?}").contains(SECRET));
        assert!(!format!("{}", config.blizzard_client_secret).contains(SECRET));
        assert!(!format!("{:?}", config.blizzard_client_secret).contains(SECRET));
    }

    #[test]
    fn a_missing_setting_is_fundamental_and_names_the_variable() {
        let dir = temp_dir("missing");
        let mut v = vars(&secret_file(&dir));
        v.remove("DP_LOCK_FILE");
        let report = read(&v).unwrap_err();
        assert_eq!(report.reason, "MISSING_SETTING");
        assert_eq!(report.metadata["variable"], "DP_LOCK_FILE");
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    }

    #[test]
    fn a_malformed_thread_count_is_refused() {
        let dir = temp_dir("threads");
        for bad in ["many", "0", "-1"] {
            let mut v = vars(&secret_file(&dir));
            v.insert("DP_ADHOC_THREADS".into(), bad.into());
            assert_eq!(read(&v).unwrap_err().reason, "MALFORMED_SETTING");
        }
    }

    #[test]
    fn an_unreadable_or_empty_secret_file_is_refused_without_the_path() {
        let dir = temp_dir("secretfile");
        let mut v = vars(&secret_file(&dir));
        v.insert(
            "DP_BLIZZARD_CLIENT_SECRET_FILE".into(),
            "/nonexistent/secret".into(),
        );
        let report = read(&v).unwrap_err();
        assert_eq!(report.reason, "SECRET_FILE_UNREADABLE");
        assert!(!format!("{report:?}").contains("/nonexistent"));

        let empty = dir.join("empty");
        std::fs::write(&empty, "\n").unwrap();
        v.insert(
            "DP_BLIZZARD_CLIENT_SECRET_FILE".into(),
            empty.to_str().unwrap().into(),
        );
        assert_eq!(read(&v).unwrap_err().reason, "SECRET_FILE_EMPTY");
    }
}
