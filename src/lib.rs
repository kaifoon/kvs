#![deny(missing_docs)]
//! key value store lib
pub use engines::{KvStore, SledStore, KvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;
pub use client::KvsClient;

mod engines;
mod error;
mod server;
mod client;
mod common;

