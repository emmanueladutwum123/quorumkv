# Build stage. Pinned to the Go version in go.mod so a container build and a
# local build cannot disagree about language semantics.
FROM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# cache. The generated protobuf code is committed, so protoc is not needed here.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off produces a static binary that runs on a scratch base. Trimpath keeps
# build machine paths out of the binary, so two builds of the same commit
# produce the same bytes.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/quorumkvd ./cmd/quorumkvd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/quorumkvctl ./cmd/quorumkvctl

# An empty directory to stamp /data from. A named volume mounted over an empty
# path inherits that path's ownership from the image, and the runtime stage has
# no shell to create it with -- so it has to be built here and copied across.
RUN mkdir -p /data-template

# Runtime stage. No shell, no package manager, nothing to exploit -- a consensus
# node needs a socket, a data directory, and nothing else.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/quorumkvd /usr/local/bin/quorumkvd
COPY --from=build /out/quorumkvctl /usr/local/bin/quorumkvctl

# The write-ahead log and snapshots live here. It must be a volume: a node that
# loses this directory has lost its vote and its acknowledged writes with it.
#
# Owned by the unprivileged user the container runs as. Without this the volume
# is created owned by root and the node cannot write its log at all -- it fails
# at startup rather than degrading, which is the right failure but a confusing
# one to debug.
COPY --from=build --chown=65532:65532 /data-template /data
VOLUME ["/data"]

EXPOSE 9000 9100

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/quorumkvd"]
CMD ["-data-dir", "/data", "-addr", "0.0.0.0:9000", "-admin-addr", "0.0.0.0:9100"]
