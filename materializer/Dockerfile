ARG GO_IMAGE=golang:1.25.6-bookworm@sha256:f4490d7b261d73af4543c46ac6597d7d101b6e1755bcdd8c5159fda7046b6b3e
FROM ${GO_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/taxi-materializer ./cmd/taxi-materializer

FROM scratch
COPY --from=build /out/taxi-materializer /taxi-materializer
ENTRYPOINT ["/taxi-materializer"]
