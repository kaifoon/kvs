use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsError, Result};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

use std::time::Duration;
/// The Client of a key value store
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Connect to server with IP:PORT address
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let addr: SocketAddr = addr.to_socket_addrs().unwrap().next().unwrap();
        let tcp_reader = TcpStream::connect_timeout(&addr, Duration::from_secs(1))
            .expect("Couldn't connect to the server...");
        //tcp_reader.set_read_timeout(Some(Duration::from_micros(100)))?;
        let tcp_writer = tcp_reader.try_clone()?;
        //tcp_writer.set_write_timeout(Some(Duration::from_micros(10)))?;

        Ok(Self {
            reader: Deserializer::new(IoRead::new(BufReader::new(tcp_reader))),
            writer: BufWriter::new(tcp_writer),
        })
    }

    /// Get the value of a given key from the server
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;

        let resp = GetResponse::deserialize(&mut self.reader)?;
        match resp {
            GetResponse::Ok(val) => Ok(val),
            GetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    /// Set the value of a string key in the server
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;

        let resp = SetResponse::deserialize(&mut self.reader)?;

        match resp {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    /// Remove a string key in the server.
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;

        let resp = RemoveResponse::deserialize(&mut self.reader)?;

        match resp {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{thread_pool::*, KvStore, KvsServer};
    use slog;
    use slog::Drain;
    use slog_term;
    use tempfile::TempDir;

    fn get_logger() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());

        logger
    }

    #[test]
    fn test_multireqs() {
        let temp_dir = TempDir::new().unwrap();
        let logger = get_logger();
        let engine = KvStore::open(temp_dir.path(), logger.clone()).unwrap();
        let pool = SharedQueueThreadPool::new(4, logger.clone()).unwrap();
        let mut server = KvsServer::new(engine, logger, pool);
        server.run("127.0.0.1:4000").unwrap();

        let mut client = KvsClient::connect("127.0.0.1:4000").unwrap();
        client.set("ad".to_string(), "adad".to_string()).unwrap();
        let mut client = KvsClient::connect("127.0.0.1:4000").unwrap();
        client.set("acd".to_string(), "adad".to_string()).unwrap();
        let mut client = KvsClient::connect("127.0.0.1:4000").unwrap();
        let ret = client.get("ad".to_string()).unwrap().unwrap();
        assert_eq!(ret, "adad".to_string());
        let mut client = KvsClient::connect("127.0.0.1:4000").unwrap();
        let re = client.get("acd".to_string()).unwrap().unwrap();
        assert_eq!(re, "adad".to_string());

        server.shutdown();
    }
}
