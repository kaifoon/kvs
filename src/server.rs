use crate::{
    common::{GetResponse, RemoveResponse, Request, SetResponse},
    KvsEngine, Result,
};
use serde_json::Deserializer;

use slog;

use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

/// The Server of a key value store
pub struct KvsServer<E: KvsEngine> {
    engine: E,
    logger: slog::Logger,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Create a `KvsServer` with a given storage engine.
    pub fn new(engine: E, logger: slog::Logger) -> Self {
        Self { engine, logger }
    }

    /// Run the server listening on the given address
    pub fn run(mut self, addr: impl ToSocketAddrs) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        slog::error!(&self.logger, "Error on serving client: {}", e);
                    }
                }
                Err(e) => slog::error!(&self.logger, "Connection failed: {}", e),
            }
        }

        Ok(())
    }

    /// Handle Request from client, do the Kvstore query and return result to client via `TcpStream`
    fn serve(&mut self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()?;
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! sent_resp {
            ( $resp:expr ) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                slog::debug!(&self.logger, "Response sent to {}: {:?}", peer_addr, resp);
            };};
        }

        for req in req_reader {
            let req = req?;
            slog::debug!(
                &self.logger,
                "Receive request from {}: {:?}",
                peer_addr,
                req
            );
            match req {
                Request::Get { key } => sent_resp!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Set { key, value } => sent_resp!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                }),
                Request::Remove { key } => sent_resp!(match self.engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                }),
            };
        }

        Ok(())
    }
}
