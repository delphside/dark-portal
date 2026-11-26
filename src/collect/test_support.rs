//! What the tests of `dp collect` and `dp items` share: a stubbed Blizzard
//! and the token cache's file.

use std::fs;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use tokio::runtime::Runtime;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::Endpoints;

pub(crate) const SECRET: &str = "the-client-secret";
pub(crate) const TOKEN: &str = "tok-new-7f3a";

/// The stubbed Blizzard: one server for both hosts, on its own runtime so the
/// blocking client under test can call it.
pub(crate) struct Stub {
    pub(crate) runtime: Runtime,
    pub(crate) server: MockServer,
}

impl Stub {
    pub(crate) fn new() -> Stub {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let server = runtime.block_on(MockServer::start());
        Stub { runtime, server }
    }

    pub(crate) fn mount(&self, mock: Mock) {
        self.runtime.block_on(mock.mount(&self.server));
    }

    pub(crate) fn endpoints(&self) -> Endpoints {
        Endpoints {
            oauth: self.server.uri(),
            api: self.server.uri(),
        }
    }

    pub(crate) fn requests(&self, verb: &str) -> usize {
        self.runtime
            .block_on(self.server.received_requests())
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == verb)
            .count()
    }

    /// The token endpoint gives `TOKEN`, up to `times` times.
    pub(crate) fn token_ok(&self, times: u64) {
        self.mount(
            Mock::given(method("POST"))
                .and(path("/token"))
                .and(header(
                    "authorization",
                    format!("Basic {}", STANDARD.encode(format!("client-id:{SECRET}"))).as_str(),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                    r#"{{"access_token":"{TOKEN}","token_type":"bearer","expires_in":86399}}"#
                )))
                .up_to_n_times(times),
        );
    }

    pub(crate) fn token_status(&self, status: u16) {
        self.mount(
            Mock::given(method("POST"))
                .and(path("/token"))
                .respond_with(ResponseTemplate::new(status)),
        );
    }
}

pub(crate) fn http_date(rfc3339: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

/// Puts a token in the cache under `data_dir`, expiring at `expires_at`.
pub(crate) fn cache_token(data_dir: &Path, value: &str, expires_at: &str) {
    let file = data_dir.join("state/blizzard-token.json");
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    let expires = chrono::DateTime::parse_from_rfc3339(expires_at)
        .unwrap()
        .timestamp();
    fs::write(
        file,
        format!(r#"{{"access_token":"{value}","expires_at":{expires}}}"#),
    )
    .unwrap();
}
