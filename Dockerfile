FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/deduper ./cmd/deduper
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/deduper /deduper
# A named volume is seeded from the image's directory (content *and*
# ownership) the first time it's mounted. distroless has no shell to chown
# at runtime, so /data has to already be owned by nonroot here — otherwise
# Docker creates the volume root-owned and the app can't open its db file.
COPY --from=build --chown=nonroot:nonroot /out/data /data
VOLUME ["/data"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/deduper"]
