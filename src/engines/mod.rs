use crate::Result;

/// The `KvsEngine` trait - defines the storage interface called by `KvsServer`
/// all KV-Store instance must implemented KvsEngine method
pub trait KvsEngine {
    /// Sets the value of a type `T` key to a `T`.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Gets the string value of a given `T` key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove a given key.
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledStore;

