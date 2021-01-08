use crate::{KvsEngine, KvsError, Result};
use sled::{Db, Tree};

/// The `SledStore` stores string key/value pairs.
#[derive(Clone)]
pub struct SledStore(Db);

impl SledStore {
    /// Construct a `SledStore` with the given path
    ///
    /// This will create `Db` instance .
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn new(db: Db) -> Self {
        Self(db)
    }
}

impl KvsEngine for SledStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key.into_bytes(), value.into_bytes())?;
        tree.flush()?;
        Ok(())
    }
    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    fn get(&self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;

        Ok(tree
            .get(key.into_bytes())?
            .map(|i_vec| i_vec.as_ref().to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }
    /// Remove a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&self, key: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.remove(key.into_bytes())?
            .ok_or(KvsError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}
