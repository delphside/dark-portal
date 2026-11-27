//! The token a run holds, and how it is taken and renewed.

use super::blizzard::Blizzard;
use super::clock::Clock;
use super::failure::{Failure, client_secret_refused};
use super::token::{Token, TokenCache};
use crate::config::Config;
use crate::settings::collection::TOKEN_MARGIN;

/// Where in [`Session::request`] a failure came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// The gate refused the request.
    Gate,
    /// Taking or storing the token, or a token Blizzard refused.
    Token,
    /// The request itself.
    Request,
}

/// A failure of [`Session::request`], and where it came from.
#[derive(Debug)]
pub(crate) struct Staged {
    pub(crate) stage: Stage,
    pub(crate) failure: Failure,
}

/// A new token Blizzard refused: the client secret is wrong.
fn secret_refused() -> Staged {
    Staged {
        stage: Stage::Token,
        failure: client_secret_refused().with_metadata("http_status", "401"),
    }
}

/// The token a run holds: the cached one, or a new one.
pub(crate) struct Session<'a> {
    config: &'a Config,
    clock: &'a dyn Clock,
    blizzard: &'a Blizzard,
    cache: TokenCache,
    token: Option<Token>,
    /// True where the token was taken from Blizzard in this run.
    fresh: bool,
}

impl<'a> Session<'a> {
    /// A session with no token held yet: the cache is read on first use.
    pub(crate) fn new(
        config: &'a Config,
        clock: &'a dyn Clock,
        blizzard: &'a Blizzard,
    ) -> Session<'a> {
        Session {
            config,
            clock,
            blizzard,
            cache: TokenCache::new(&config.data_dir),
            token: None,
            fresh: false,
        }
    }

    /// Runs `send` with a token, taking a new one once where the cached one
    /// is refused (401). `gate` runs before every request to Blizzard, the
    /// token's included, and may refuse it. A new token that is refused too is
    /// the client secret being wrong, *technical, fundamental*.
    pub(crate) fn request<T>(
        &mut self,
        gate: &mut dyn FnMut() -> Result<(), Failure>,
        send: &dyn Fn(&Blizzard, &Token) -> Result<T, Failure>,
    ) -> Result<T, Staged> {
        let staged = |stage| move |failure| Staged { stage, failure };
        let token = self.token(gate)?;
        gate().map_err(staged(Stage::Gate))?;
        match send(self.blizzard, &token) {
            Err(Failure::Refused) if !self.fresh => {
                tracing::info!("the cached token was refused; taking a new one");
                gate().map_err(staged(Stage::Gate))?;
                let token = self.renew().map_err(staged(Stage::Token))?;
                gate().map_err(staged(Stage::Gate))?;
                match send(self.blizzard, &token) {
                    Err(Failure::Refused) => Err(secret_refused()),
                    other => other.map_err(staged(Stage::Request)),
                }
            }
            Err(Failure::Refused) => Err(secret_refused()),
            other => other.map_err(staged(Stage::Request)),
        }
    }

    /// The token to use: the cached one while it has more than the token
    /// margin left, otherwise a new one.
    fn token(&mut self, gate: &mut dyn FnMut() -> Result<(), Failure>) -> Result<Token, Staged> {
        let now = self.clock.now();
        if self.token.is_none() {
            self.token = self.cache.load();
        }
        if let Some(token) = &self.token
            && token.has_more_than(TOKEN_MARGIN, now)
        {
            return Ok(token.clone());
        }
        gate().map_err(|failure| Staged {
            stage: Stage::Gate,
            failure,
        })?;
        self.renew().map_err(|failure| Staged {
            stage: Stage::Token,
            failure,
        })
    }

    /// Takes a new token and caches it.
    fn renew(&mut self) -> Result<Token, Failure> {
        let token = self.blizzard.token(
            &self.config.blizzard_client_id,
            &self.config.blizzard_client_secret,
            self.clock.now(),
        )?;
        self.cache.store(&token)?;
        self.token = Some(token.clone());
        self.fresh = true;
        tracing::info!("took a new Blizzard access token");
        Ok(token)
    }
}
