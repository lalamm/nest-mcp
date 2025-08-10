# Use the latest Rust image for building
FROM rust:1.89.0 AS builder

# Set the working directory
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached if dependencies don't change)
RUN cargo build --release && rm src/main.rs

# Copy the source code
COPY src ./src

# Build the application
RUN cargo build --release

# Use a minimal runtime image
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -s /bin/bash appuser

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/nest-mcp /app/nest-mcp

# Copy the parquet data file
COPY hello_nest.parquet /app/hello_nest.parquet

# Change ownership to the non-root user
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Expose the port (Cloud Run will set PORT environment variable)
EXPOSE 8080

# Set environment variables for Cloud Run
ENV PORT=8080
ENV RUST_LOG=info

# Run the application
CMD ["./nest-mcp", "serve"]