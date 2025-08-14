# syntax=docker/dockerfile:1
FROM golang:1.25 as builder
ENV GOTOOLCHAIN=local \
    GIT_TERMINAL_PROMPT=0
WORKDIR /app
COPY go.mod go.sum ./
COPY vendor/ ./vendor/
COPY . .
# Build with vendor to avoid network during build
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /out/agile-pulse ./cmd/api

FROM alpine:3.20
RUN adduser -D -u 65532 appuser && apk add --no-cache ca-certificates tzdata
WORKDIR /
COPY --from=builder /out/agile-pulse /agile-pulse
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/agile-pulse"]

