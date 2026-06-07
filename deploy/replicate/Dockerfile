# Build context must be the repo root (luxfi/zapdb).
# docker build -t zapdb-replicate -f deploy/replicate/Dockerfile .
FROM golang:1.26.4-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/zapdb-replicate \
      ./cmd/replicate

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/zapdb-replicate /usr/local/bin/zapdb-replicate
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/zapdb-replicate"]
CMD ["replicate", "--config", "/etc/zapdb-replicate/config.yaml"]
