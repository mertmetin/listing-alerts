# One Dockerfile for every Go service.
#
#   docker build --build-arg SERVICE=api .   one service; runs as /service
#   docker build .                           every service in one image, one
#                                            compile (/poller, /detector, ...);
#                                            what the small production VPS uses
#
# The build stage runs on the builder's own CPU and cross-compiles for the
# target, so a Mac can also build x86 images:
#   docker buildx build --platform linux/amd64 .
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
# -p 2 caps parallel compiles so a 4 GB machine can build this.
RUN export GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH}; \
    LDFLAGS="-s -w -X github.com/mertmetin/listing-alerts/internal/platform.Version=${VERSION}"; \
    mkdir -p /out && \
    if [ -n "$SERVICE" ]; then \
      go build -p 2 -trimpath -ldflags "$LDFLAGS" -o /out/service ./cmd/${SERVICE}; \
    else \
      for s in poller detector matcher api delivery apikey; do \
        go build -p 2 -trimpath -ldflags "$LDFLAGS" -o /out/$s ./cmd/$s || exit 1; \
      done; \
    fi

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/service"]
