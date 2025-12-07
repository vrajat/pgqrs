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

# Releases

Releases to crates.io are automated via GitHub Actions triggered by git tags.

## Steps to Release

1. **Update version in Cargo.toml**
   ```bash
   # Install cargo-release tool if not already installed
   cargo install cargo-release
   # Bump version (patch, minor, or major)
   cargo release patch  # or minor/major
   # OR manually edit version in Cargo.toml
   ```

2. **Create and push git tag**
   ```bash
   # Tag must follow semantic versioning (e.g., v0.3.0)
   git tag v0.3.0
   git push origin v0.3.0
   ```

3. **Automated release process**
   - Git tag triggers GitHub Actions workflow
   - Workflow runs tests and builds
   - On success, publishes to crates.io


## Version Bumping Guidelines

- **Patch** (`0.1.0` → `0.1.1`): Bug fixes, documentation updates
- **Minor** (`0.1.0` → `0.2.0`): New features, backward compatible
- **Major** (`0.1.0` → `1.0.0`): Breaking changes

## Pre-release Checklist

- [ ] All tests pass (`cargo test`)
- [ ] Code formatted (`cargo fmt`)
- [ ] Documentation updated
- [ ] Changelog updated (if applicable)
- [ ] CI passes on main branch
- [ ] Version bumped in Cargo.toml
- [ ] Reviewed breaking changes (if any)