# pgqrs-macros

> [!CAUTION]
> **Internal Crate**: This crate (`pgqrs-macros`) contains procedural macros for the main `pgqrs` library.
> Prefer using `pgqrs`, which re-exports these macros.
>
> See the [pgqrs crate](https://crates.io/crates/pgqrs) for full documentation.

## What is pgqrs-macros?

`pgqrs-macros` provides the `#[pgqrs_workflow]` and `#[pgqrs_step]` macros used by the Rust workflow API. It does not include runtime logic or queue storage—those live in `pgqrs`.

## Installation

```toml
[dependencies]
pgqrs = "0.14.0"
pgqrs-macros = "0.14.0"
```

## Usage (Rust)

```rust
use pgqrs;
use pgqrs_macros::{pgqrs_workflow, pgqrs_step};

#[pgqrs_step]
async fn fetch_data(ctx: &pgqrs::Workflow, url: &str) -> Result<String, anyhow::Error> {
    Ok(reqwest::get(url).await?.text().await?)
}

#[pgqrs_workflow]
async fn data_pipeline(ctx: &pgqrs::Workflow, url: &str) -> Result<String, anyhow::Error> {
    let data = fetch_data(ctx, url).await?;
    Ok(format!("Processed {} bytes", data.len()))
}
```

## Documentation

- **[Full Documentation](https://pgqrs.vrajat.com)** - Complete guides and API reference
- **[Rust API Docs](https://docs.rs/pgqrs)** - Rust crate documentation

## License

[MIT](https://choosealicense.com/licenses/mit/)
