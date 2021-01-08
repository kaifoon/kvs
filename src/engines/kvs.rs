use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use slog;

use std::cell::RefCell;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsEngine, KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1 << 20;
const CACHE_LEN: usize = 1 << 10;

/// The `KvStore` stores string key/value pairs.
///
/// Key/Value pairs are stored in a `HashMap` in memory and not persisted to disk
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, Result, KvsEngine};
/// # use std::env::current_dir;
/// # use slog;
/// # use slog_term;
/// # use slog::Drain;
/// # fn try_main() -> Result<()> {
///    let decorator = slog_term::TermDecorator::new().build();
///    let drain = slog_term::FullFormat::new(decorator).build().fuse();
///    let drain = std::sync::Mutex::new(drain).fuse();
///    let logger = slog::Logger::root(drain, slog::o!());
///    let mut store = KvStore::open(current_dir()?, logger)?;
///    store.set("key".to_owned(), "value".to_owned())?;
///    let val = store.get("key".to_owned())?;
///    assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
///
#[derive(Clone)]
pub struct KvStore {
    // directory for the log and other data
    path: Arc<PathBuf>,
    // logger
    logger: slog::Logger,
    // map generation number to the file reader.
    index: Arc<DashMap<String, CommandPos>>,
    writer: Arc<Mutex<KvStoreWriter>>,
    reader: KvStoreReader,
    // LRUCache
    cache: Arc<Mutex<LRUCache>>,
}

impl KvStore {
    /// Opens a `KvStore` with the given path
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>, logger: slog::Logger) -> Result<Self> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let index = Arc::new(DashMap::new());

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &*index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen)?;
        let safe_point = Arc::new(AtomicU64::new(0));
        let cache = LRUCache::new(CACHE_LEN);

        let reader = KvStoreReader {
            path: path.clone(),
            safe_point,
            readers: RefCell::new(readers),
        };
        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            current_gen,
            uncompacted,
            path: path.clone(),
            index: index.clone(),
            logger: logger.clone(),
        };

        Ok(Self {
            path,
            logger,
            index,
            reader,
            writer: Arc::new(Mutex::new(writer)),
            cache: Arc::new(Mutex::new(cache)),
        })
    }
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&self, key: String, value: String) -> Result<()> {
        // set cache `key-value` first;
        self.cache.lock().unwrap().set(key.clone(), value.clone());
        self.writer
            .lock()
            .expect("Writer accquire lock to set key-value failed")
            .set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    fn get(&self, key: String) -> Result<Option<String>> {
        // get `value` from cache first
        if let Some(val) = self.cache.lock().unwrap().get(key.clone()) {
            return Ok(Some(val));
        }

        match self.index.get(&key) {
            Some(cmd_pos) => {
                if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            }
            None => Ok(None),
        }
    }

    /// Remove a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&self, key: String) -> Result<()> {
        // remove `key-value` cache first
        self.cache.lock().unwrap().remove(key.clone());
        self.writer
            .lock()
            .expect("Writer accquire lock to set key-value failed")
            .remove(key)
    }
}

/// A single thread reader
/// Each `KvStore` instance has its own `KvStoreReader` and
/// `KvStoreReader`s open the same files separately. So the user
/// can read concurrently through multiple `KvStore`s in different
/// threads
struct KvStoreReader {
    path: Arc<PathBuf>,
    // generation of the latest compaction file
    safe_point: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,
}

impl KvStoreReader {
    /// Close file handles with generation number less than safe_point
    ///
    /// `safe_point` is updated to the latest compaction gen after a compaction finishes.
    /// The compaction generation contains the sum of all operations before it and the
    /// in-memory index contains no entries with generation number less than safe_point.
    /// So we can safely close those file handles and the stale files can be deleted.
    fn close_stale_handles(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let first_gen = *readers.keys().next().unwrap();
            if self.safe_point.load(Ordering::SeqCst) <= first_gen {
                break;
            }
            readers.remove(&first_gen);
        }
    }

    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>,
    {
        // Open the file if we haven't opened it in this `KvStoreReader`.
        // We don't use entry API here because we want the errors to be propogated.

        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_pos.gen) {
            let reader = BufReaderWithPos::new(File::open(log_path(&self.path, cmd_pos.gen))?)?;
            readers.insert(cmd_pos.gen, reader);
        }
        let reader = readers
            .get_mut(&cmd_pos.gen)
            .expect("Cannot find log reader");
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }

    fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        self.read_and(cmd_pos, |cmd_reader| {
            Ok(serde_json::from_reader(cmd_reader)?)
        })
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> KvStoreReader {
        KvStoreReader {
            path: self.path.clone(),
            safe_point: self.safe_point.clone(),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    // writer of the current log
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    // the number of bytes representing "stable" commands that could be
    // deleted during a compaction.
    uncompacted: u64,
    path: Arc<PathBuf>,
    index: Arc<DashMap<String, CommandPos>>,
    logger: slog::Logger,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;

        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.get(&key) {
                self.uncompacted += old_cmd.value().len;
            }
            self.index
                .insert(key, (self.current_gen, pos..self.writer.pos).into());
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            let pos = self.writer.pos;

            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            if let Command::Remove { key } = cmd {
                let (_, old_cmd) = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
                // the "remove" command itself can be deleted in the next compaction
                // so we add its length to `uncompacted`
                self.uncompacted += self.writer.pos - pos;
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
    /// Clears stale entries in the log
    fn compact(&mut self) -> Result<()> {
        // increase current gen by 2 current_gen + 1 is for the compaction file.
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = new_log_file(&self.path, self.current_gen)?;

        let mut compaction_writer = new_log_file(&self.path, compaction_gen)?;

        let mut new_pos = 0; // pos in the new log file.
        for mut entry in self.index.iter_mut() {
            let len = self.reader.read_and(*entry.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut compaction_writer)?)
            })?;

            *entry = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }

        compaction_writer.flush()?;

        self.reader
            .safe_point
            .store(compaction_gen, Ordering::SeqCst);
        self.reader.close_stale_handles();

        // remove stale log files
        // Note that actually these files are not deleted immediately because `KvStoreReader`s
        // still keep open file handles. When `KvStoreReader` is used next time, it will clear
        // its stale file handles. On Unix, the files will be deleted after all the handles
        // are closed. On windows, the deletions below will fail and stale files are expected
        // to be deleted in the next compaction
        let stale_gens = sorted_gen_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compaction_gen);

        for stale_gen in stale_gens {
            let file_path = log_path(&self.path, stale_gen);
            if let Err(e) = fs::remove_file(&file_path) {
                slog::error!(self.logger, "{:?} cannot be deleted: {}", &file_path, e);
            }
        }

        self.uncompacted = 0;

        Ok(())
    }
}
/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(path: &Path, gen: u64) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;

    Ok(writer)
}

/// Returns sorted generation numbers in the given directory.
///
/// # Error
/// path not exist return `io::error::Error`
///
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    gen_list.sort_unstable();

    Ok(gen_list)
}

/// Load the whole log file and store value locations in the index map
///
/// Returns how many bytes can be saved after a compaction
fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &DashMap<String, CommandPos>,
) -> Result<u64> {
    // To make sure we read from the beginning of the file
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.get(&key) {
                    uncompacted += old_cmd.value().len;
                }
                index.insert(key, (gen, pos..new_pos).into());
            }
            Command::Remove { key } => {
                if let Some((_, old_cmd)) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }

                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }

    Ok(uncompacted)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// Struct representing a command.
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

/// Represents the position and length of a json-serialized command in the log
#[derive(Copy, Clone, Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

/// Cache use LRU (Least Recented Used) evict policy
struct LRUCache {
    dq: VecDeque<String>,
    map: HashMap<String, String>,
    cap: usize,
}

impl LRUCache {
    ///  Initialize the LRU cache with positive size `capacity` .
    fn new(capacity: usize) -> Self {
        Self {
            dq: VecDeque::with_capacity(capacity),
            map: HashMap::with_capacity(capacity),
            cap: capacity,
        }
    }

    ///  Return the value of the key if the key exists
    fn get(&mut self, key: String) -> Option<String> {
        if let Some(n) = self.dq.iter().position(|x| x == &key) {
            self.dq.remove(n);
        }

        let val = self.map.get(&key).map(|s| s.to_string());
        self.dq.push_front(key);

        val
    }

    /// Update the value of the `key` if the `key` exists. Otherwise,
    /// add the `key-value` pair to the cache. If the number of keys exceeds the `capacity` from
    /// this operation, evict the least recently used key.
    fn set(&mut self, key: String, value: String) {
        if let Some(n) = self.dq.iter().position(|x| x == &key) {
            self.dq.remove(n);
        }

        if self.cap == self.map.len() {
            if let Some(key) = self.dq.pop_back() {
                self.map.remove(&key);
            }
        }

        self.dq.push_front(key.clone());
        self.map.insert(key, value);
    }

    /// Remove key-value when `KvStore::remove` method called
    ///
    /// Return None while key is not exist
    fn remove(&mut self, key: String) {
        if let Some(n) = self.dq.iter().position(|x| x == &key) {
            self.dq.remove(n);
        }
        self.map.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_gen_sort_list() -> Result<()> {
        use std::env::current_dir;
        let ret = sorted_gen_list(&current_dir()?)?;
        assert_eq!(ret, vec![1, 2, 3, 4, 5]);
        Ok(())
    }

    #[test]
    fn test_dashmap() {
        let ret = DashMap::new();
        ret.insert(
            "csd".to_string(),
            CommandPos {
                gen: 2,
                pos: 4,
                len: 10,
            },
        );
        if let Some(v) = ret.get("csd") {
            assert_eq!(2, v.gen);
            assert_eq!(4, v.pos);
            assert_eq!(10, v.len);
        }
        if let Some(v) = ret.insert(
            "csd".to_string(),
            CommandPos {
                gen: 3,
                pos: 4,
                len: 6,
            },
        ) {
            assert_eq!(2, v.gen);
            assert_eq!(4, v.pos);
            assert_eq!(10, v.len);
        }

        if let Some(v) = ret.get("csd") {
            assert_eq!(3, v.gen);
            assert_eq!(4, v.pos);
            assert_eq!(6, v.len);
        };
    }
}
