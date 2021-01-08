use crate::Result;
use super::ThreadPool;
use std::thread;
use slog;

/// It is actually not a thread pool. It spawns a new thread every time
/// the `spawn` method is called.
#[derive(Clone)]
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32, _logger: slog::Logger) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
