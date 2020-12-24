#![deny(missing_docs)]
//! key value store lib
pub use kv::KvStore;
pub use error::{KvsError, Result};

mod kv;
mod error;
