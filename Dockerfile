# ========================
# Build Stage
# ========================
FROM rust:1.90.0-alpine3.22@sha256:b4b54b176a74db7e5c68fdfe6029be39a02ccbcfe72b6e5a3e18e2c61b57ae26 AS builder

# Build platform argument (x86_64 or aarch64) (default: x86_64)
ARG TARGETARCH
RUN echo "TARGETARCH: $TARGETARCH"

# Install build dependencies, including static OpenSSL libraries
RUN apk add --no-cache \
    musl-dev \
    openssl-dev \
    openssl-libs-static \
    pkgconfig \
    build-base \
    curl \
    git

# Set PATH only if we installed the cross compiler (will be empty string for x86)
ENV PATH="$(cat /tmp/musl_cross_path):$PATH"

# Set environment variables for static linking with OpenSSL
ENV OPENSSL_STATIC=yes
ENV OPENSSL_LIB_DIR=/usr/lib
ENV OPENSSL_INCLUDE_DIR=/usr/include

# Set the working directory
WORKDIR /usr/src/app

# Copy over Cargo.toml and Cargo.lock for dependency caching
COPY Cargo.toml Cargo.lock ./

# Copy over all the source code
COPY . .

# Build the project in release mode for the MUSL target
RUN cargo build --locked --release --bin nexusd

# Strip the binaries to reduce size
RUN strip target/release/nexusd

# ========================
# Runtime Stage
# ========================
FROM alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f

ARG TARGETARCH

# Install runtime dependencies. Image variants are made from JPEG, PNG, GIF and WebP only
# (PNG and GIF ship with the base package), so no other ImageMagick coder package is installed.
RUN apk add --no-cache ca-certificates \
    imagemagick \
    imagemagick-webp \
    imagemagick-jpeg

# Restrictive ImageMagick policy: coder allowlist, no delegates, resource limits.
COPY docker/imagemagick/policy.xml /etc/ImageMagick-7/policy.xml

# Unprivileged runtime user. The config dir must be readable and the files_path writable by it
# (UID/GID 10001); the default config dir is ~/.pubky-nexus under its home.
RUN addgroup -S -g 10001 nexus \
    && adduser -S -D -u 10001 -G nexus -h /home/nexus nexus

# Copy the compiled binaries from the builder stage
COPY --from=builder /usr/src/app/target/release/nexusd /usr/local/bin/nexusd

# Set the working directory
WORKDIR /usr/local/bin

USER 10001:10001

# Expose the port the service listens on
EXPOSE 8080

# Set the default command to run the service binary
CMD ["nexusd"]
