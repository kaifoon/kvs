use crate::thread_pool::ThreadPool;
use crate::{
    common::{GetResponse, RemoveResponse, Request, SetResponse},
    KvsEngine, Result,
};
use serde_json::Deserializer;

use slog;
use slog_term;
use slog::Drain;

use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// The Server of a key value store
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    logger: slog::Logger,
    pool: P,
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with a given storage engine.
    pub fn new(engine: E, logger: slog::Logger, pool: P) -> Self {
        Self {
            engine,
            logger,
            pool,
            handle: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Run the server listening on the given address
    pub fn run(&mut self, addr: impl ToSocketAddrs) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        let shutdown = self.shutdown.clone();
        let engine = self.engine.clone();
        let logger = self.logger.clone();
        let pool = self.pool.clone();

        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let engine = engine.clone();
                        pool.spawn(|| {
                            if let Err(e) = serve(engine, stream) {
                                eprintln!("Error on serving client: {}", e);
                            }
                        });
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(e) => slog::error!(logger, "Connection failed: {}", e),
                }
            }
        });

        self.handle.replace(handle);
        Ok(())
    }
    /// Shutdown the server
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let handle = self.handle.take().unwrap();
        handle.join().unwrap();
    }
}

/// Handle Request from client, do the Kvstore query and return result to client via `TcpStream`
fn serve<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, slog::o!());

    macro_rules! sent_resp {
        ( $resp:expr ) => {{
            let resp = $resp;
            serde_json::to_writer(&mut writer, &resp)?;
            writer.flush()?;
            slog::debug!(logger, "Response sent to {}: {:?}", peer_addr, resp);
        };};
    }

    for req in req_reader {
        let req = req?;
        slog::debug!(logger, "Receive request from {}: {:?}", peer_addr, req);
        match req {
            Request::Get { key } => sent_resp!(match engine.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(e) => GetResponse::Err(format!("{}", e)),
            }),
            Request::Set { key, value } => sent_resp!(match engine.set(key, value) {
                Ok(_) => SetResponse::Ok(()),
                Err(e) => SetResponse::Err(format!("{}", e)),
            }),
            Request::Remove { key } => sent_resp!(match engine.remove(key) {
                Ok(_) => RemoveResponse::Ok(()),
                Err(e) => RemoveResponse::Err(format!("{}", e)),
            }),
        };
    }

    Ok(())
}
