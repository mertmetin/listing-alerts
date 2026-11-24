# One Dockerfile for every Go service: `docker build --build-arg SERVICE=api .`
#
# The build stage runs on the builder's own CPU and cross-compiles for the
# target (Go needs no emulator), so a Mac can build x86 images for the VPS:
#   docker buildx build --platform linux/amd64 --build-arg SERVICE=api .
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
RUN test -n "$SERVICE" && GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags "-s -w -X github.com/mertmetin/listing-alerts/internal/platform.Version=${VERSION}" \
      -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/service"]
