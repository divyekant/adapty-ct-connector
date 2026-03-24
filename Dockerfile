FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /connector ./cmd/connector
RUN CGO_ENABLED=0 go build -o /backfill ./cmd/backfill

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /connector /usr/local/bin/connector
COPY --from=builder /backfill /usr/local/bin/backfill
COPY transform-config.json /etc/connector/transform-config.json
ENTRYPOINT ["connector"]
