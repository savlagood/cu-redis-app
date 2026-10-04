FROM rust:1.91-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/gamehub /usr/local/bin/gamehub
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gamehub"]
