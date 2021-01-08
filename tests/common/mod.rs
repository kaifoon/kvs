use slog;
use slog_term;
use slog::Drain;

pub fn _setup() {
    println!("Starting Integration tests...");
}

pub fn setup_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = std::sync::Mutex::new(drain).fuse();
    slog::Logger::root(drain, slog::o!())
}
