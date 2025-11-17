# Contributing

Thank you for your interest in contributing to pgqrs!

## Development Environment

1. Clone the repository
2. Run tests: `cargo test`
3. Run with PostgreSQL: Set `PGQRS_TEST_DSN=postgresql://user:pass@localhost:5432/db`

---

## Building and Viewing Documentation (Development Mode)

To build and view the documentation locally with live reload:

```bash
make docs
```

This will:
- Generate the Rust API documentation and copy it to `docs/api/`.
- Start the MkDocs development server at `http://127.0.0.1:8000` (default).
- Watch for changes and automatically reload the site as you edit Markdown files or Rust code.

## Building Release Documentation

To build the production-ready documentation (output in `site/`):

```bash
make docs-release
```

## Cleaning Generated Documentation

To remove all generated documentation (API and site):

```bash
make clean-docs
```

---

## Releases

This project uses [release-plz](https://release-plz.dev/) for automated releases:

- **Automatic releases**: When changes are merged to `main`, release-plz automatically:
  - Updates the version in `Cargo.toml` based on conventional commits
  - Updates the `CHANGELOG.md`
  - Creates a Git tag and GitHub release
  - Publishes to [crates.io](https://crates.io) via trusted publishing

- **Commit format**: Use [conventional commits](https://www.conventionalcommits.org/) for automatic version bumping:
  - `feat:` for new features (minor version bump)
  - `fix:` for bug fixes (patch version bump)
  - `feat!:` or `BREAKING CHANGE:` for breaking changes (major version bump)
  - `docs:`, `refactor:`, `perf:`, etc. for other changes

- **Manual releases**: Run the "Release-plz" workflow manually from the GitHub Actions tab
