use crate::{Result, KvsError};
use super::ThreadPool;
use rayon;
use slog;
use std::sync::Arc;


/// Wrapper of rayon::ThreadPool
#[derive(Clone)]
pub struct RayonThreadPool(Arc<rayon::ThreadPool>);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32, _logger: slog::Logger) -> Result<Self> {
      let pool = rayon::ThreadPoolBuilder::new().num_threads(threads as usize).build().map_err(|e| KvsError::StringError(format!("{}", e)))?;
      Ok(RayonThreadPool(Arc::new(pool)))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
      self.0.spawn(job)
    }
}

