//! What the tests of more than one job share: capturing what a run logs.

use std::io;
use std::sync::{Arc, Mutex};

/// A log writer that keeps what it is given.
#[derive(Clone)]
struct Logged(Arc<Mutex<Vec<u8>>>);

impl io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Runs `f` and gives what it logged.
pub(crate) fn logging<T>(f: impl FnOnce() -> T) -> (T, String) {
    // `tracing` caches whether a log call has a listener. A test that logs
    // with none listening would turn the call off for the tests that capture
    // it, so a global listener that discards everything keeps every call on.
    static LISTENER: std::sync::Once = std::sync::Once::new();
    LISTENER.call_once(|| {
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::fmt().with_writer(io::sink).finish(),
        );
    });
    let log = Logged(Arc::default());
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let result = tracing::subscriber::with_default(subscriber, f);
    let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    (result, text)
}
