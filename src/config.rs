//! Deployment settings, read from the environment variables listed in
//! `docs/5.4.1-configuration.md`. A missing or
//! malformed one is a *technical, fundamental* error: the operator fixes the
//! environment's `.env`.

use std::fmt;
use std::path::PathBuf;

use crate::error::{ErrorType, Report};

/// A secret: its `Debug` and `Display` never print it, so it cannot reach a
/// log by accident. It is never logged, nor written to bronze or the export
/// (`docs/5.4.1-configuration.md`).
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
    /// `DP_BUCKET_ACCESS_FILE`'s contents: what gives write access to the
    /// bucket. Set where `bucket` is. What it is, and how it is used, is
    /// `crate::daily::transport`'s (docs/5.1.3).
    pub bucket_access: Option<Secret>,
    /// `DP_LOCK_FILE`: the VM-wide lock every writing job takes.
    pub lock_file: PathBuf,
    /// `DP_ADHOC_MEMORY_LIMIT`: DuckDB's `memory_limit` for an ad hoc query.
    pub adhoc_memory_limit: String,
    /// `DP_ADHOC_THREADS`: DuckDB's `threads` for an ad hoc query.
    pub adhoc_threads: u32,
}

/// A secret from the file at `path`, without its trailing newline. `what`
/// names it in a message; `variable` is the setting that names the file. The
/// path is never in the report.
fn read_secret(path: &str, variable: &str, what: &str) -> Result<Secret, Box<Report>> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        let mut report = fundamental(
            "SECRET_FILE_UNREADABLE",
            &format!("{what} file cannot be read; check its path and its owner."),
            variable,
        );
        report
            .metadata
            .insert("kind".to_owned(), format!("{:?}", e.kind()));
        report
    })?;
    let text = text.trim_end_matches(['\n', '\r']);
    if text.is_empty() {
        return Err(fundamental(
            "SECRET_FILE_EMPTY",
            &format!("{what} file is empty; put the secret in it."),
            variable,
        ));
    }
    Ok(Secret::new(text))
}

fn fundamental(reason: &str, message: &str, variable: &str) -> Box<Report> {
    Box::new(
        Report::fatal(ErrorType::TechnicalFundamental, reason, message)
            .with_metadata("variable", variable),
    )
}

impl Config {
    /// Reads the settings from the process's environment.
    ///
    /// # Errors
    ///
    /// As [`from_lookup`](Config::from_lookup).
    pub fn from_env() -> Result<Config, Box<Report>> {
        Config::from_lookup(|name| std::env::var(name).ok())
    }

    /// Reads the settings through `lookup`, which gives a variable's value or
    /// `None`; [`from_env`](Config::from_env) passes the environment, a test
    /// passes a map. Reads the secret from the file `DP_BLIZZARD_CLIENT_SECRET_FILE`
    /// names, without its trailing newline.
    ///
    /// # Errors
    ///
    /// A *technical, fundamental* fatal, exit `2`, naming the variable, where a
    /// required variable is missing or empty, `DP_ADHOC_THREADS` is not a
    /// positive whole number, or the secret file cannot be read or is empty.
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

        let secret = read_secret(
            &required("DP_BLIZZARD_CLIENT_SECRET_FILE")?,
            "DP_BLIZZARD_CLIENT_SECRET_FILE",
            "The Blizzard client secret",
        )?;
        let bucket = lookup("DP_BUCKET").filter(|v| !v.is_empty());
        // A bucket without the means to write to it would fail every night.
        let bucket_access = match bucket {
            Some(_) => Some(read_secret(
                &required("DP_BUCKET_ACCESS_FILE")?,
                "DP_BUCKET_ACCESS_FILE",
                "The bucket access secret",
            )?),
            None => None,
        };

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
            blizzard_client_secret: secret,
            bucket,
            bucket_access,
            lock_file: PathBuf::from(required("DP_LOCK_FILE")?),
            adhoc_memory_limit: required("DP_ADHOC_MEMORY_LIMIT")?,
            adhoc_threads,
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
            ("DP_BUCKET_ACCESS_FILE", secret_path.to_str().unwrap()),
            ("DP_LOCK_FILE", "/srv/dp/dp.lock"),
            ("DP_ADHOC_MEMORY_LIMIT", "4GB"),
            ("DP_ADHOC_THREADS", "1"),
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
        assert_eq!(config.bucket_access.unwrap().expose(), SECRET);
        assert_eq!(config.env, "production");
        assert_eq!(config.data_dir, PathBuf::from("/srv/dp/production"));
        assert_eq!(config.blizzard_client_id, "client-id");
        assert_eq!(config.blizzard_client_secret.expose(), SECRET);
        assert_eq!(config.bucket.as_deref(), Some("bucket"));
        assert_eq!(config.lock_file, PathBuf::from("/srv/dp/dp.lock"));
        assert_eq!(config.adhoc_memory_limit, "4GB");
        assert_eq!(config.adhoc_threads, 1);
    }

    #[test]
    fn daily_bucket_access_file_is_read_only_where_a_bucket_is_set() {
        let dir = temp_dir("bucket");
        let mut v = vars(&secret_file(&dir));
        v.insert("DP_BUCKET".into(), String::new());
        // With no bucket there is nothing to write to: its access is not read.
        v.remove("DP_BUCKET_ACCESS_FILE");
        let config = read(&v).unwrap();
        assert_eq!((config.bucket, config.bucket_access), (None, None));
        v.remove("DP_BUCKET");
        assert_eq!(read(&v).unwrap().bucket, None);
        // Unread with no bucket: a file that is not there is no error.
        v.insert("DP_BUCKET_ACCESS_FILE".into(), "/nonexistent/access".into());
        let config = read(&v).unwrap();
        assert_eq!((config.bucket, config.bucket_access), (None, None));
        v.insert("DP_BUCKET".into(), String::new());
        assert!(read(&v).unwrap().bucket_access.is_none());
    }

    #[test]
    fn a_bucket_without_its_access_is_refused_as_fundamental() {
        let dir = temp_dir("bucket-access");
        let mut v = vars(&secret_file(&dir));
        v.remove("DP_BUCKET_ACCESS_FILE");
        let report = read(&v).unwrap_err();
        assert_eq!(report.reason, "MISSING_SETTING");
        assert_eq!(report.metadata["variable"], "DP_BUCKET_ACCESS_FILE");
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);

        v.insert("DP_BUCKET_ACCESS_FILE".into(), "/nonexistent/access".into());
        let report = read(&v).unwrap_err();
        assert_eq!(report.reason, "SECRET_FILE_UNREADABLE");
        assert_eq!(report.metadata["variable"], "DP_BUCKET_ACCESS_FILE");
        assert!(!format!("{report:?}").contains("/nonexistent"));
    }

    #[test]
    fn the_secret_is_never_printed() {
        let dir = temp_dir("print");
        let config = read(&vars(&secret_file(&dir))).unwrap();
        assert!(config.bucket_access.is_some());
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
