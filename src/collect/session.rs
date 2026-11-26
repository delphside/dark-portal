//! The token a run holds, and how it is taken and renewed.

use super::blizzard::Blizzard;
use super::clock::Clock;
use super::failure::Failure;
use super::token::{Token, TokenCache};
use crate::config::Config;
use crate::settings::collection::TOKEN_MARGIN;

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

    /// The client the session takes its token through.
    pub(crate) fn blizzard(&self) -> &'a Blizzard {
        self.blizzard
    }

    /// Whether the token was taken from Blizzard in this run, so a refusal of
    /// it is the client secret being wrong and not a stale cache.
    pub(crate) fn is_fresh(&self) -> bool {
        self.fresh
    }

    /// The token to use: the cached one while it has more than the token
    /// margin left, otherwise a new one.
    pub(crate) fn token(&mut self) -> Result<Token, Failure> {
        let now = self.clock.now();
        if self.token.is_none() {
            self.token = self.cache.load();
        }
        if let Some(token) = &self.token
            && token.has_more_than(TOKEN_MARGIN, now)
        {
            return Ok(token.clone());
        }
        self.renew()
    }

    /// Takes a new token and caches it.
    pub(crate) fn renew(&mut self) -> Result<Token, Failure> {
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
