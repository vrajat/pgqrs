# Multi-stage Dockerfile for pgqrs CLI binary
FROM rust:1.88-slim-bookworm AS builder

WORKDIR /usr/src/pgqrs
COPY . .

# Build only the core pgqrs CLI binary in release mode
RUN cargo build --release -p pgqrs

# Production runtime stage
FROM debian:bookworm-slim

# Copy built binary from builder stage
COPY --from=builder /usr/src/pgqrs/target/release/pgqrs /usr/local/bin/pgqrs

# Set entrypoint to pgqrs binary
ENTRYPOINT ["pgqrs"]
