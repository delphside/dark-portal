//! How `dp daily` puts a file in the bucket: the one place that knows the
//! mechanism (5.1.3 *bucket transport*, `docs/5.1.3-technology-decisions.md`).
//!
//! **Provisional.** The owner is choosing the mechanism. For now a file is put
//! with one HTTPS `PUT` to an Object Storage *pre-authenticated request* URL:
//! the secret in `DP_BUCKET_ACCESS_FILE` is the URL of the bucket's folder (the
//! one `dp` is to write below), and an object's name follows it. The URL carries
//! its own authority, so it is a secret and is never logged, nor is an error's
//! text, which a library may build from it. Changing the mechanism replaces
//! [`Par`] and [`from_config`], and nothing else.

use std::cell::Cell;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use ureq::{Agent, SendBody};

use crate::collect::failure::{Failure, io_failure, is_blip_status};
use crate::config::{Config, Secret};
use crate::error::ErrorType;
use crate::settings::storage::BUCKET_PUSH_TIMEOUT;

/// Puts files in the bucket.
pub(crate) trait Transport {
    /// Puts `file` in the bucket as `object`, a name with `/` between its
    /// parts, replacing any object of that name. One attempt: a failure the
    /// caller may try again after a wait (a timeout, the bucket unreachable,
    /// a 429, 502, 503 or 504) is a [`Failure::Retry`], carrying the
    /// bucket's `Retry-After` where it gave one. Otherwise a failure's type
    /// says what to do: *fundamental* is the access ([`ACCESS_REFUSED`]) or
    /// the file, and the rest, a 500 included, is *unexpected*.
    fn put(&self, object: &str, file: &Path) -> Result<(), Failure>;
}

/// The reason of a failure that the access is not valid, which every later
/// object would meet.
pub(crate) const ACCESS_REFUSED: &str = "BUCKET_ACCESS_REFUSED";

/// The transport for the configured bucket: `None` where the environment has
/// none.
pub(crate) fn from_config(config: &Config) -> Option<Box<dyn Transport>> {
    config.bucket.as_ref()?;
    let access = config.bucket_access.clone()?;
    Some(Box::new(Par::new(access, BUCKET_PUSH_TIMEOUT)))
}

/// `PUT` to a pre-authenticated request URL.
pub(crate) struct Par {
    base: Secret,
    agent: Agent,
}

impl Par {
    /// A transport that puts objects below `base`, giving each request
    /// `timeout` in all, upload included.
    pub(crate) fn new(base: Secret, timeout: Duration) -> Par {
        Par {
            base,
            agent: Agent::config_builder()
                .timeout_global(Some(timeout))
                .http_status_as_error(false)
                .build()
                .into(),
        }
    }

    fn url(&self, object: &str) -> String {
        format!(
            "{}/{}",
            self.base.expose().trim_end_matches('/'),
            percent_encoded(object)
        )
    }
}

/// An object's name as a URL path: a byte that is not unreserved and not `/`
/// is written as `%XX`.
fn percent_encoded(object: &str) -> String {
    let mut out = String::with_capacity(object.len());
    for byte in object.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// A file read for the upload, which keeps the error that reading it gave:
/// the HTTP library reports it as an I/O error like the network's, and the
/// two are told apart by this.
struct Watched<'a> {
    file: &'a File,
    failed: &'a Cell<Option<std::io::Error>>,
}

impl Read for Watched<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf).inspect_err(|e| {
            self.failed
                .set(Some(std::io::Error::new(e.kind(), "reading the file")));
        })
    }
}

impl Transport for Par {
    fn put(&self, object: &str, file: &Path) -> Result<(), Failure> {
        let file = File::open(file).map_err(|e| io_failure(&e, "opening a file to push"))?;
        let length = file
            .metadata()
            .map_err(|e| io_failure(&e, "reading the size of a file to push"))?
            .len();
        let failed = Cell::new(None);
        let mut reader = Watched {
            file: &file,
            failed: &failed,
        };
        let sent = self
            .agent
            .put(self.url(object))
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", length.to_string())
            .send(SendBody::from_reader(&mut reader));
        // A read error is the file's, whatever the library made of it.
        if let Some(error) = failed.take() {
            return Err(io_failure(&error, "reading a file to push"));
        }
        // Its text is not kept: it may name the URL.
        let response = sent.map_err(|e| transport(&e))?;
        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        Err(refusal(
            status,
            crate::collect::blizzard::retry_after(&response),
        ))
    }
}

fn transport(error: &ureq::Error) -> Failure {
    match error {
        ureq::Error::Timeout(_) => Failure::blip(
            "BUCKET_TIMEOUT",
            "The bucket did not answer in time; the push tries again, and then the next run does.",
            None,
        ),
        ureq::Error::Io(_) | ureq::Error::ConnectionFailed | ureq::Error::HostNotFound => {
            Failure::blip(
                "BUCKET_UNREACHABLE",
                "The bucket could not be reached; the push tries again, and then the next run does.",
                None,
            )
        }
        _ => Failure::unexpected(
            "BUCKET_REQUEST_FAILED",
            "The request to the bucket could not be made.",
        ),
    }
}

/// Sorts an HTTP status that is not a success; `after` is the `Retry-After`
/// the bucket gave.
fn refusal(status: u16, after: Option<Duration>) -> Failure {
    let failure = match status {
        // The access is not valid: expired, revoked or for another bucket.
        401 | 403 | 404 => Failure::stop(
            ErrorType::TechnicalFundamental,
            ACCESS_REFUSED,
            "The bucket refused the push; check the bucket access in DP_BUCKET_ACCESS_FILE.",
        ),
        // Busy or down for now: the push asks again after the wait.
        _ if is_blip_status(status) => Failure::blip(
            "BUCKET_BUSY",
            "The bucket answered that it is busy or down for now; the push tries again, and then the next run does.",
            after,
        ),
        _ => Failure::unexpected(
            "BUCKET_UNEXPECTED_STATUS",
            "The bucket answered the push with an unexpected status.",
        ),
    };
    failure.with_metadata("http_status", status.to_string())
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{body_bytes, header, method, path};
    use wiremock::{Mock, ResponseTemplate};

    use super::*;
    use crate::collect::test_support::Stub;
    use crate::error::{ExitStatus, exit_status};

    const TOKEN: &str = "par-token-4f2a";

    fn par(stub: &Stub) -> Par {
        Par::new(
            Secret::new(format!("{}/p/{TOKEN}/o/", stub.server.uri())),
            Duration::from_secs(5),
        )
    }

    fn file(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("dp-transport-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("object.parquet");
        std::fs::write(&file, bytes).unwrap();
        file
    }

    fn refused(failure: Failure) -> crate::error::Report {
        match failure {
            Failure::Stop(report) | Failure::Retry { report, .. } => *report,
            other => panic!("expected a stop or a retry, got {other:?}"),
        }
    }

    #[test]
    fn a_file_is_put_whole_to_its_object_with_its_length() {
        // BK: one `PUT` of the file's bytes, with a `Content-Length`, to the
        // access's address below which the object's name follows.
        let stub = Stub::new();
        stub.mount(
            Mock::given(method("PUT"))
                .and(path(format!("/p/{TOKEN}/o/bronze/items/a.parquet")))
                .and(header("content-length", "9"))
                .and(body_bytes(b"123456789".to_vec()))
                .respond_with(ResponseTemplate::new(200))
                .expect(1),
        );
        par(&stub)
            .put("bronze/items/a.parquet", &file("whole", b"123456789"))
            .unwrap();
        assert_eq!(stub.requests("PUT"), 1);
    }

    #[test]
    fn a_refused_access_is_fundamental_and_never_names_the_access() {
        // 401, 403 and 404 are what an expired or wrong address gives.
        for status in [401, 403, 404] {
            let stub = Stub::new();
            stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(status)));
            let report = refused(
                par(&stub)
                    .put("a/b.parquet", &file("refused", b"x"))
                    .unwrap_err(),
            );
            assert_eq!(report.reason, ACCESS_REFUSED, "{status}");
            assert_eq!(exit_status(&report), ExitStatus::Fundamental);
            assert_eq!(report.metadata["http_status"], status.to_string());
            assert!(!format!("{report:?}").contains(TOKEN));
        }
    }

    #[test]
    fn a_busy_or_down_bucket_is_a_blip_and_any_other_status_is_unexpected() {
        for status in [429, 502, 503, 504] {
            let stub = Stub::new();
            stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(status)));
            let report = refused(
                par(&stub)
                    .put("a.parquet", &file("busy", b"x"))
                    .unwrap_err(),
            );
            assert_eq!(
                report.error_type,
                Some(ErrorType::TechnicalBlip),
                "{status}"
            );
        }
        // A 500 is not a blip: the 1.10 type table makes it unexpected.
        for status in [408, 418, 500] {
            let stub = Stub::new();
            stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(status)));
            let report = refused(par(&stub).put("a.parquet", &file("odd", b"x")).unwrap_err());
            assert_eq!(report.error_type, Some(ErrorType::Unexpected), "{status}");
        }
    }

    #[test]
    fn a_blip_status_is_asked_again_after_the_retry_after_and_the_others_are_not() {
        for status in [429, 502, 503, 504] {
            let stub = Stub::new();
            stub.mount(
                Mock::given(method("PUT"))
                    .respond_with(ResponseTemplate::new(status).insert_header("Retry-After", "7")),
            );
            let failure = par(&stub)
                .put("a.parquet", &file("retry-after", b"x"))
                .unwrap_err();
            let Failure::Retry { report, after } = failure else {
                panic!("{status}: expected a retry");
            };
            assert_eq!(after, Some(Duration::from_secs(7)), "{status}");
            assert_eq!(report.error_type, Some(ErrorType::TechnicalBlip));
        }
        let stub = Stub::new();
        stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(429)));
        let failure = par(&stub)
            .put("a.parquet", &file("no-retry-after", b"x"))
            .unwrap_err();
        assert!(matches!(failure, Failure::Retry { after: None, .. }));
        for status in [408, 500, 418] {
            let stub = Stub::new();
            stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(status)));
            let failure = par(&stub)
                .put("a.parquet", &file("no-retry", b"x"))
                .unwrap_err();
            assert!(matches!(failure, Failure::Stop(_)), "{status}");
        }
    }

    #[test]
    fn a_timeout_and_an_unreachable_bucket_are_asked_again() {
        let nobody = Par::new(
            Secret::new(format!("http://127.0.0.1:1/p/{TOKEN}/o/")),
            Duration::from_secs(5),
        );
        let failure = nobody
            .put("a.parquet", &file("retry-down", b"x"))
            .unwrap_err();
        assert!(matches!(failure, Failure::Retry { .. }), "{failure:?}");
        let stub = Stub::new();
        stub.mount(
            Mock::given(method("PUT"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3))),
        );
        let slow = Par::new(
            Secret::new(format!("{}/p/{TOKEN}/o/", stub.server.uri())),
            Duration::from_millis(300),
        );
        let failure = slow
            .put("a.parquet", &file("retry-slow", b"x"))
            .unwrap_err();
        assert!(matches!(failure, Failure::Retry { .. }), "{failure:?}");
    }

    #[test]
    fn a_bucket_that_cannot_be_reached_or_is_too_slow_is_a_blip() {
        let nobody = Par::new(
            Secret::new(format!("http://127.0.0.1:1/p/{TOKEN}/o/")),
            Duration::from_secs(5),
        );
        let report = refused(nobody.put("a.parquet", &file("down", b"x")).unwrap_err());
        assert_eq!(report.error_type, Some(ErrorType::TechnicalBlip));
        assert!(!format!("{report:?}").contains(TOKEN));

        let stub = Stub::new();
        stub.mount(
            Mock::given(method("PUT"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3))),
        );
        let slow = Par::new(
            Secret::new(format!("{}/p/{TOKEN}/o/", stub.server.uri())),
            Duration::from_millis(300),
        );
        let report = refused(slow.put("a.parquet", &file("slow", b"x")).unwrap_err());
        assert_eq!(report.reason, "BUCKET_TIMEOUT");
        assert!(!format!("{report:?}").contains(TOKEN));
    }

    #[test]
    fn a_file_that_is_not_there_is_a_local_failure_and_asks_the_bucket_nothing() {
        let stub = Stub::new();
        let report = refused(
            par(&stub)
                .put("a.parquet", Path::new("/nonexistent/object.parquet"))
                .unwrap_err(),
        );
        assert_eq!(report.error_type, Some(ErrorType::TechnicalFundamental));
        assert_eq!(stub.requests("PUT"), 0);
    }

    #[test]
    fn a_file_that_cannot_be_read_mid_upload_is_a_local_failure_not_the_buckets() {
        // A folder opens as a file and fails at its first read, which the
        // HTTP library reports as an I/O error like a lost connection's.
        let stub = Stub::new();
        stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(200)));
        let folder = file("unreadable", b"x");
        let report = refused(
            par(&stub)
                .put("a.parquet", folder.parent().unwrap())
                .unwrap_err(),
        );
        assert_eq!(report.reason, "LOCAL_IO_FAILED");
        assert_eq!(report.error_type, Some(ErrorType::TechnicalFundamental));
    }

    #[test]
    fn there_is_a_transport_only_where_the_environment_has_a_bucket_and_its_access() {
        let mut config = crate::transform::test_support::World::new("transport-config")
            .config
            .clone();
        assert!(from_config(&config).is_none());
        config.bucket = Some("bucket".to_owned());
        assert!(from_config(&config).is_none());
        config.bucket_access = Some(Secret::new("https://x.invalid/p/t/o/"));
        assert!(from_config(&config).is_some());
        config.bucket = None;
        assert!(from_config(&config).is_none());
    }

    #[test]
    fn an_object_name_is_a_path_and_anything_odd_is_escaped() {
        assert_eq!(
            percent_encoded("bronze/commodities/eu/2026-11-26T10.parquet"),
            "bronze/commodities/eu/2026-11-26T10.parquet"
        );
        assert_eq!(
            percent_encoded("a b/c?d#e.parquet"),
            "a%20b/c%3Fd%23e.parquet"
        );
    }

    #[test]
    fn an_object_goes_below_the_base_whether_or_not_it_ends_in_a_slash() {
        for base in ["https://store.test/p/tok/o/", "https://store.test/p/tok/o"] {
            let par = Par::new(Secret::new(base), Duration::from_secs(1));
            assert_eq!(
                par.url("daily/item.parquet"),
                "https://store.test/p/tok/o/daily/item.parquet"
            );
        }
    }
}
