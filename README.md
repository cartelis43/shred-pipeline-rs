# Shred Pipeline Rust Implementation

This project implements the Receiver and Decoder layers as specified in the Shred Pipeline specification. It is designed to handle incoming data, process it, and validate it according to the defined protocols.

## Project Structure

- `Cargo.toml`: Configuration file for the Rust project.
- `.gitignore`: Specifies files and directories to be ignored by Git.
- `src/lib.rs`: Main library entry point, declaring public interfaces and modules.
- `src/receiver/mod.rs`: Module for the Receiver layer, containing public exports.
- `src/receiver/receiver.rs`: Implementation of the Receiver layer.
- `src/decoder/mod.rs`: Module for the Decoder layer, containing public exports.
- `src/decoder/decoder.rs`: Implementation of the Decoder layer.
- `src/types.rs`: Common types and data structures used throughout the project.
- `src/error.rs`: Custom error types and error handling logic.
- `tests/integration.rs`: Integration tests for the project.

## Building the Project

To build the project, run the following command in the project root:

```
cargo build
```

## Running Tests

To run the tests, use the following command:

```
cargo test
```

## Usage

To use the Receiver and Decoder layers, you can import them in your Rust code as follows:

```rust
use shred_pipeline_rs::receiver::Receiver;
use shred_pipeline_rs::decoder::Decoder;
```

You can then create instances of `Receiver` and `Decoder` and use their methods to handle and process data according to the Shred Pipeline specification.