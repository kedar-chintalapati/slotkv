FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY . .
# Cache mounts keep the module download and compile caches between builds.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/kvnode ./cmd/kvbench

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvnode /out/kvbench /
ENTRYPOINT ["/kvnode"]
