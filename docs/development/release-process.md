# Release Process

How pgqrs releases are managed and published.

## Version Scheme

pgqrs follows [Semantic Versioning](https://semver.org/):

- **MAJOR** - Breaking API changes
- **MINOR** - New features, backwards compatible
- **PATCH** - Bug fixes, backwards compatible

Example: `1.2.3`

## Release Artifacts

Each release publishes:

| Artifact | Location |
|----------|----------|
| Rust crate | [crates.io/crates/pgqrs](https://crates.io/crates/pgqrs) |
| Python package | [PyPI pgqrs](https://pypi.org/project/pgqrs/) |
| Documentation | [pgqrs.com](https://pgqrs.com) |
| GitHub Release | [Releases](https://github.com/vrajat/pgqrs/releases) |

## Release Checklist

### 1. Prepare Release

```bash
# Ensure main is up to date
git checkout main
git pull origin main

# Create release branch
git checkout -b release/v1.2.3
```

### 2. Update Version Numbers

**Rust crates:**

```toml
# Cargo.toml (workspace)
[workspace.package]
version = "1.2.3"

# crates/pgqrs/Cargo.toml
version = "1.2.3"

# py-pgqrs/Cargo.toml
version = "1.2.3"
```

**Python package:**

```toml
# py-pgqrs/pyproject.toml
[project]
version = "1.2.3"
```

### 3. Update Changelog

Add release notes to `CHANGELOG.md`:

```markdown
## [1.2.3] - 2024-01-15

### Added
- New feature description

### Changed
- Changed behavior description

### Fixed
- Bug fix description

### Deprecated
- Deprecated feature warning

### Removed
- Removed feature notice

### Security
- Security fix description
```

### 4. Run Tests

```bash
# Full test suite
cargo test

# Python tests
cd py-pgqrs && maturin develop && pytest

# Build release
cargo build --release
```

### 5. Create Pull Request

```bash
git add -A
git commit -m "chore: prepare release v1.2.3"
git push origin release/v1.2.3
```

Create PR and get approval.

### 6. Merge and Tag

```bash
# After PR approval
git checkout main
git pull origin main

# Create tag
git tag -a v1.2.3 -m "Release v1.2.3"
git push origin v1.2.3
```

### 7. Publish

**Rust crate:**

```bash
cd crates/pgqrs
cargo publish
```

**Python package:**

```bash
cd py-pgqrs
maturin build --release
maturin publish
```

### 8. Create GitHub Release

1. Go to [Releases](https://github.com/vrajat/pgqrs/releases)
2. Click "Create release"
3. Select the tag
4. Copy changelog entry to description
5. Publish release

### 9. Update Documentation

Documentation is automatically deployed from `main` branch.

## Automated Releases (CI)

The project uses GitHub Actions for automated releases:

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  publish-rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo publish --token ${{ secrets.CARGO_TOKEN }}

  publish-python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with:
          command: publish
          args: --token ${{ secrets.PYPI_TOKEN }}
```

## Pre-release Versions

For testing before official release:

```bash
# Alpha release
git tag -a v1.2.3-alpha.1 -m "Alpha release"

# Beta release
git tag -a v1.2.3-beta.1 -m "Beta release"

# Release candidate
git tag -a v1.2.3-rc.1 -m "Release candidate"
```

## Hotfix Process

For urgent fixes to released versions:

```bash
# Create hotfix branch from tag
git checkout -b hotfix/v1.2.4 v1.2.3

# Make fix
# Update version to 1.2.4
# Commit and push

# Create PR to main
# After merge, tag and release
```

## Changelog Format

Follow [Keep a Changelog](https://keepachangelog.com/):

```markdown
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Upcoming features

## [1.2.3] - 2024-01-15

### Added
- Added delayed message support (#123)
- Added batch enqueue for Python bindings (#124)

### Fixed
- Fixed connection pool exhaustion under load (#125)

### Changed
- Improved error messages for configuration errors

[Unreleased]: https://github.com/vrajat/pgqrs/compare/v1.2.3...HEAD
[1.2.3]: https://github.com/vrajat/pgqrs/compare/v1.2.2...v1.2.3
```

## Release Communication

1. **GitHub Release** - Detailed changelog
2. **Crates.io** - Rust community
3. **PyPI** - Python community
4. **Documentation** - Updated for new features
