# syntax=docker/dockerfile:1
FROM golang:1.25 as builder
ENV GOTOOLCHAIN=local \
    GIT_TERMINAL_PROMPT=0
WORKDIR /app
COPY go.mod go.sum ./
COPY . .
# Build with module mode and use a reliable public proxy, fallback to direct
ENV GOPROXY=https://goproxy.cn,direct GOSUMDB=off
RUN CGO_ENABLED=0 GOOS=linux go build -mod=mod -o /out/agile-pulse ./cmd/api

FROM alpine:3.20
RUN adduser -D -u 65532 appuser && apk add --no-cache ca-certificates tzdata
WORKDIR /
COPY --from=builder /out/agile-pulse /agile-pulse
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/agile-pulse"]

