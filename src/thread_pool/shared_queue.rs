use std::thread;

use super::ThreadPool;
use crate::Result;
use slog;

use crossbeam_channel::{unbounded, Receiver, Sender};

// Note for Rust training course: the thread pool is not implemented using
// `catch_unwind` because it would require the task to be `UnwindSafe`.

fn run_tasks(tsk: TaskReceiver) {
    loop {
        match tsk.rx.recv() {
            Ok(task) => {
                task();
            }
            Err(_) => slog::debug!(tsk.logger, "Thread exits because the thread pool is destroyed."),
        }
    }
}
/// A thread pool using a shared queue inside.
///
/// If a spawned task panics, the old thread will be destroyed and a new one will be
/// created. It fails silently when any failure to create the thread at the OS level
/// is captured after the thread pool is created. So, the thread number in the pool
/// can decrease to zero, then spawning a task to the thread pool will panic.
#[derive(Clone)]
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32, logger: slog::Logger) -> Result<Self> {
        let (tx, rx) = unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        for _ in 0..threads {
            let rx = TaskReceiver {rx: rx.clone(), logger: logger.clone()};
            thread::Builder::new().spawn(move || run_tasks(rx))?;
        }
        Ok(SharedQueueThreadPool { tx })
    }

    /// Spawns a function into the thread pool.
    ///
    /// # Panics
    ///
    /// Panics if the thread pool has no thread.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx
            .send(Box::new(job))
            .expect("The thread pool has no thread.");
    }
}

#[derive(Clone)]
struct TaskReceiver {
  rx: Receiver<Box<dyn FnOnce() + Send + 'static>>,
  logger: slog::Logger,
}

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            if let Err(e) = thread::Builder::new().spawn(move || run_tasks(rx)) {
                slog::error!(self.logger, "Failed to spawn a thread: {}", e);
            }
        }
    }
}

