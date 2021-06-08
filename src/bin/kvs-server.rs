use clap::{App, Arg};
use kvs::thread_pool::*;
use kvs::*;
use num_cpus;

#[macro_use]
extern crate slog;
extern crate slog_term;

use sled;
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;

use slog::Drain;
use std::thread;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: &str = "kvs";

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("addr")
                .takes_value(true)
                .short("a")
                .long("addr")
                .default_value(DEFAULT_ADDR)
                .help("An IP address with the format IP:PORT.default 127.0.0.1:4000."),
        )
        .arg(
            Arg::with_name("engine")
                .takes_value(true)
                .short("e")
                .long("engine")
                .help("Specifing custome Key-Value Store engine, default KvStore."),
        )
        .get_matches();

    // IP Addr
    let addr = matches.value_of("addr").unwrap();
    // todo 如果打开的数据是 其他的存储引擎的话，改成其他.
    let engine = matches.value_of("engine");

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, o!());

    match current_engine(&logger) {
        Ok(Some(e)) => {
            if engine.is_none() || e == engine.unwrap() {
                run(e.as_str(), addr, logger)
            } else {
                error!(logger, "Wrong Engine!");
                exit(1);
            }
        }
        Ok(None) => {
            let e = engine.unwrap_or(DEFAULT_ENGINE);
            // write engine to engine file
            fs::write(current_dir()?.join("engine"), format!("{}", e))?;
            run(e, addr, logger)
        }
        Err(e) => {
            error!(logger, "{}", e);
            exit(1);
        }
    }
}


fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr, logger: slog::Logger) -> Result<()> {
    let pool = RayonThreadPool::new(num_cpus::get() as u32, logger.clone())?;
    let mut server = KvsServer::new(engine, logger, pool);
    server.run(addr)?;
    loop {
      thread::park()
    }
}

fn run(engine: &str, addr: &str, logger: slog::Logger) -> Result<()> {
    info!(
        logger,
        "kvs-server {} Storage engine: {} Listening on {}",
        env!("CARGO_PKG_VERSION"),
        engine,
        addr
    );
    // write engine to engine file
    fs::write(current_dir()?.join("engine"), engine.to_string())?;
    // parse address to SocketAddr Type, use `unwrap` method because of cli set default argument
    let addr = addr.parse::<SocketAddr>().unwrap();

    if engine == "kvs" {
        run_with_engine(KvStore::open(current_dir()?, logger.clone())?, addr, logger)
    } else if engine == "sled" {
        run_with_engine(
            SledStore::new(sled::open(current_dir()?.join("sled_store"))?),
            addr,
            logger,
        )
    } else {
        Ok(())
    }
}
fn current_engine(logger: &slog::Logger) -> Result<Option<String>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine) {
        Ok(eng) => Ok(Some(eng)),
        Err(e) => {
            warn!(logger, "The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
