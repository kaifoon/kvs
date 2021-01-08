use super::ThreadPool;
use crate::Result;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// `SharedQueueThreadPool` runs jobs (functions) on a set of reusable threads,
/// which can be more efficient than spawning a new thread for every job.
///
/// A single shared queue to distribute work to idle threads. `producer` , the
/// thread accepts network connections, sends jobs to a single queue , and the
/// `consummers`, every idle thread in the pool, read from that channel waiting for
/// a job to execute.
pub struct SharedQueueThreadPool {
    threads: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

struct Worker {
    id: u32,
    handle: Option<thread::JoinHandle<()>>,
}

enum Message {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        assert!(threads > 0);

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::with_capacity(threads as usize);
        for id in 0..threads {
            workers.push(Worker::new(id, rx.clone()));
        }

        Ok(Self {
            threads: workers,
            sender: tx,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let message = Message::Job(Box::new(job));
        self.sender
            .send(message)
            .expect("receiver has already been deallocated.");
    }
}

fn handle_panic(err: ) {
  unimplemented!()
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in &mut self.threads {
            self.sender.send(Message::Shutdown);
        }

        for worker in &mut self.threads {
            println!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.handle.take() {
                thread.join().map_err(handle_panic);
            }
        }

        Ok(())
    }
}


impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver
                .lock()
                .expect("another user of this mutex panicked while holding the mutex")
                .recv()
                .expect("corresponding sender has disconnected.");
            match message {
                Message::Job(job) => {
                    job();
                }
                Message::Shutdown => {
                    break;
                }
            }
            println!("{}", id);
        });
        Self {
            id,
            handle: Some(thread),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_cpus;
    use crate::Result;

    #[test]
    #[ignore]
    fn test_thread_message() {
        // send job to idle thread

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));

        let mut workers = vec![];
        for id in 0..3 {
            workers.push(Worker::new(id, rx.clone()));
        }

        tx.send(Message::Job(Box::new(|| println!("job sent.."))));
    }
    #[test]
    #[ignore]
    fn test_threadpool() -> Result<()> {
        let pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
        pool.spawn(|| println!("job sent.."));

        // thread panic
        // 如果某线程panic, 则该线程杀死，重新起过新线程
        Ok(())
    }
}
