//! Blizzard's two services as `dp collect` uses them: the OAuth token and
//! the commodities snapshot. Each answer is sorted into the failures of
//! 5.2.1 *When it fails*, `docs/5.2.1-collection.md`.

use std::io::Read;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use ureq::Agent;

use super::failure::{Failure, client_secret_refused, is_blip_status};
use super::token::Token;
use crate::config::Secret;
use crate::error::ErrorType;
use crate::settings::collection::{
    HTTP_TIMEOUT_OTHER, HTTP_TIMEOUT_SNAPSHOT, ITEM_LOCALE, ITEM_NAMESPACE, NAMESPACE,
};
use crate::types::ItemId;

/// Blizzard's base URLs. A test points them at a stub; a run uses
/// [`Endpoints::blizzard`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    /// The OAuth host, without a trailing slash.
    pub oauth: String,
    /// The Game Data API host, without a trailing slash.
    pub api: String,
}

impl Endpoints {
    /// Blizzard's own, for the EU region.
    pub fn blizzard() -> Endpoints {
        Endpoints {
            oauth: "https://oauth.battle.net".to_owned(),
            api: "https://eu.api.blizzard.com".to_owned(),
        }
    }
}

/// The two request timeouts.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub snapshot: Duration,
    pub other: Duration,
}

impl Timeouts {
    /// The settings' own.
    pub const SETTINGS: Timeouts = Timeouts {
        snapshot: HTTP_TIMEOUT_SNAPSHOT,
        other: HTTP_TIMEOUT_OTHER,
    };
}

/// A snapshot response whose headers are read and whose body is not.
pub struct SnapshotResponse {
    /// The `last-modified` header.
    pub last_modified: DateTime<Utc>,
    /// The body, to be read as a stream.
    pub body: Box<dyn Read + Send>,
}

/// What Blizzard answered to an item lookup that is data and not a failure.
#[derive(Debug, PartialEq, Eq)]
pub enum ItemAnswer {
    /// 200: the body, as Blizzard sent it.
    Found(String),
    /// 404: Blizzard has no data for the item yet.
    NotFound,
}

/// The client.
pub struct Blizzard {
    endpoints: Endpoints,
    snapshot_agent: Agent,
    other_agent: Agent,
}

fn agent(timeout: Duration) -> Agent {
    Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build()
        .into()
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// The wait a `Retry-After` of whole seconds gives.
pub(crate) fn retry_after(response: &ureq::http::Response<ureq::Body>) -> Option<Duration> {
    let seconds = response
        .headers()
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(seconds))
}

/// A transport failure: a timeout or a broken connection is a blip.
fn transport(error: &ureq::Error) -> Failure {
    // Blizzard's answer kept: the transport error's own text, which names no
    // header and no body.
    transport_kind(error).with_metadata("transport_error", error.to_string())
}

fn transport_kind(error: &ureq::Error) -> Failure {
    match error {
        ureq::Error::Timeout(_) => {
            Failure::blip("BLIZZARD_TIMEOUT", "Blizzard did not answer in time.", None)
        }
        ureq::Error::Io(_) | ureq::Error::ConnectionFailed | ureq::Error::HostNotFound => {
            Failure::blip(
                "BLIZZARD_UNREACHABLE",
                "Blizzard could not be reached.",
                None,
            )
        }
        _ => Failure::unexpected(
            "BLIZZARD_REQUEST_FAILED",
            "The request to Blizzard could not be made.",
        ),
    }
}

/// Sorts an HTTP status that is not a success.
fn status_failure(status: u16, after: Option<Duration>, what: &str) -> Failure {
    // Blizzard's answer kept: its status, never a header or a body.
    status_kind(status, after, what).with_metadata("http_status", status.to_string())
}

fn status_kind(status: u16, after: Option<Duration>, what: &str) -> Failure {
    match status {
        _ if is_blip_status(status) => Failure::blip(
            "BLIZZARD_BUSY",
            "Blizzard answered that it is busy or down for now.",
            after,
        ),
        401 => Failure::Refused,
        403 | 404 => Failure::stop(
            ErrorType::TechnicalFundamental,
            "BLIZZARD_REFUSED",
            &format!("Blizzard refused the {what} request; check the client's access."),
        ),
        _ => Failure::unexpected(
            "BLIZZARD_UNEXPECTED_STATUS",
            &format!("Blizzard answered the {what} request with an unexpected status."),
        ),
    }
}

impl Blizzard {
    /// A client for `endpoints`, with the settings' timeouts.
    pub fn new(endpoints: Endpoints) -> Blizzard {
        Blizzard::with_timeouts(endpoints, Timeouts::SETTINGS)
    }

    /// As [`new`](Blizzard::new), with other timeouts, for a test.
    pub fn with_timeouts(endpoints: Endpoints, timeouts: Timeouts) -> Blizzard {
        Blizzard {
            endpoints,
            snapshot_agent: agent(timeouts.snapshot),
            other_agent: agent(timeouts.other),
        }
    }

    /// The time each kind of request is given in all, as the agents hold it:
    /// the snapshot's, then the others'.
    #[cfg(test)]
    pub(crate) fn timeouts_held(&self) -> (Option<Duration>, Option<Duration>) {
        let held = |agent: &Agent| agent.config().timeouts().global;
        (held(&self.snapshot_agent), held(&self.other_agent))
    }

    /// Takes a token by the client-credentials grant. A refusal (401) is the
    /// wrong client secret: *technical, fundamental*.
    pub fn token(
        &self,
        client_id: &str,
        client_secret: &Secret,
        now: DateTime<Utc>,
    ) -> Result<Token, Failure> {
        let credentials = STANDARD.encode(format!("{client_id}:{}", client_secret.expose()));
        let response = self
            .other_agent
            .post(format!("{}/token", self.endpoints.oauth))
            .header("Authorization", format!("Basic {credentials}"))
            .send_form([("grant_type", "client_credentials")])
            .map_err(|e| transport(&e))?;
        let status = response.status().as_u16();
        if status == 401 {
            return Err(client_secret_refused().with_metadata("http_status", "401"));
        }
        if status != 200 {
            return Err(status_failure(status, retry_after(&response), "token"));
        }
        let text = response
            .into_body()
            .read_to_string()
            .map_err(|e| transport(&e))?;
        let parsed: TokenResponse = serde_json::from_str(&text).map_err(|_| {
            Failure::unexpected(
                "TOKEN_RESPONSE_UNUSABLE",
                "Blizzard's token response could not be read.",
            )
        })?;
        Ok(Token::new(
            Secret::new(parsed.access_token),
            now,
            Duration::from_secs(parsed.expires_in),
        ))
    }

    /// Asks for the commodities snapshot and reads its `last-modified`.
    pub fn snapshot(&self, token: &Token) -> Result<SnapshotResponse, Failure> {
        let url = format!(
            "{}/data/wow/auctions/commodities?namespace={NAMESPACE}",
            self.endpoints.api
        );
        let response = self
            .snapshot_agent
            .get(url)
            .header("Authorization", format!("Bearer {}", token.expose()))
            .call()
            .map_err(|e| transport(&e))?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(status_failure(status, retry_after(&response), "snapshot"));
        }
        let last_modified = response
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| DateTime::parse_from_rfc2822(v).ok())
            .map(|d| d.to_utc())
            .ok_or_else(|| {
                Failure::unexpected(
                    "LAST_MODIFIED_UNUSABLE",
                    "Blizzard's snapshot has no usable last-modified header.",
                )
            })?;
        Ok(SnapshotResponse {
            last_modified,
            body: Box::new(response.into_body().into_reader()),
        })
    }

    /// Asks for an item's data. A 200 gives its body unchanged, once it is
    /// known to be a JSON object, and a 404 is [`ItemAnswer::NotFound`]: data,
    /// not a failure. Every other answer is sorted as the snapshot's are.
    pub fn item(&self, token: &Token, item: ItemId) -> Result<ItemAnswer, Failure> {
        let url = format!(
            "{}/data/wow/item/{}?namespace={ITEM_NAMESPACE}&locale={ITEM_LOCALE}",
            self.endpoints.api, item.0
        );
        let response = self
            .other_agent
            .get(url)
            .header("Authorization", format!("Bearer {}", token.expose()))
            .call()
            .map_err(|e| transport(&e))?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(ItemAnswer::NotFound);
        }
        if status != 200 {
            return Err(status_failure(status, retry_after(&response), "item"));
        }
        let text = response
            .into_body()
            .read_to_string()
            .map_err(|e| transport(&e))?;
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(serde_json::Value::Object(_)) => Ok(ItemAnswer::Found(text)),
            _ => Err(Failure::unexpected(
                "ITEM_BODY_UNUSABLE",
                "Blizzard's item response could not be read.",
            )
            .with_metadata("http_status", "200")),
        }
    }
}
