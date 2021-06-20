# Naive Key-Value Database in Rust

A training course about practical systems software construction in Rust.

Learned by PingCap [Talent-Plan](https://github.com/pingcap/talent-plan/blob/master/courses/rust/README.md)


## Code Structure
```
.
 |-Cargo.lock
 |-.gitignore
 |-README.md
 |-benches
 | |-engine.rs
 | |-thread_pool.rs
 |-src
 | |-bin
 | | |-kvs-server.rs
 | | |-kvs-client.rs
 | | |-cli.yml
 | |-error.rs
 | |-thread_pool
 | | |-.shared_queue.rs
 | | |-mod.rs
 | | |-naive.rs
 | | |-shared_queue.rs
 | | |-rayon.rs
 | |-engines
 | | |-sled.rs
 | | |-kvs.rs
 | | |-mod.rs
 | |-common.rs
 | |-lib.rs
 | |-server.rs
 | |-client.rs
 |-Cargo.toml
 |-tests
 | |-common
 | | |-mod.rs
 | |-cli.rs
 | |-kv_store.rs
 | |-thread_pool.rs
 |-LICENSE
```

Finished four projects:

- [x] [Project 1: The Rust toolbox](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-1)
- [x] [Project 2: Log-structured file I/O](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-2)
- [x] [Project 3: Synchronous client-server networking](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-3)
- [x] [Project 4: Concurrency and parallelism](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-4)
- [ ] [Project 5: Asynchrony](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-5)

