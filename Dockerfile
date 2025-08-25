# ---- build ----
FROM rust:1.89-alpine AS builder
RUN apk add --no-cache musl-dev build-base ca-certificates
WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN rustup target add x86_64-unknown-linux-musl
# Build deps against MUSL
RUN cargo build --release --target x86_64-unknown-linux-musl && rm src/main.rs

# Build app (DuckDB bundled will compile C/C++ into the static binary)
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-musl

# ---- run ----
FROM gcr.io/distroless/static:nonroot
WORKDIR /app

# App binary
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/nest-mcp /app/nest-mcp

# Optional: seed DB file (read-only in root FS). For writes, move/point to /tmp at runtime.
COPY nest_mcp.db /app/nest_mcp.db

# CA bundle for HTTPS if your app/DuckDB needs it
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENV PORT=8080 RUST_LOG=info
EXPOSE 8080
USER nonroot
CMD ["/app/nest-mcp","serve"]
