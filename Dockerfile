FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /bin/service ./cmd/${SERVICE}

FROM debian:bookworm-slim
COPY --from=builder /bin/service /bin/service
ENTRYPOINT ["/bin/service"]