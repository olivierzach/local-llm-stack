FROM alpine:3.20

ARG AICHAT_VERSION=v0.30.0
ARG TARGETARCH

RUN apk add --no-cache ca-certificates curl tar \
  && case "$TARGETARCH" in \
    amd64) aichat_arch="x86_64-unknown-linux-musl" ;; \
    arm64) aichat_arch="aarch64-unknown-linux-musl" ;; \
    *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
  esac \
  && curl -fsSL "https://github.com/sigoden/aichat/releases/download/${AICHAT_VERSION}/aichat-${AICHAT_VERSION}-${aichat_arch}.tar.gz" -o /tmp/aichat.tar.gz \
  && tar -xzf /tmp/aichat.tar.gz -C /usr/local/bin aichat \
  && chmod +x /usr/local/bin/aichat \
  && rm -f /tmp/aichat.tar.gz \
  && apk del curl tar

WORKDIR /workspace
ENTRYPOINT ["aichat"]
