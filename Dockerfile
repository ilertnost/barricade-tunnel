FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o tunnel-server

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/tunnel-server /tunnel-server
EXPOSE 8081
CMD ["/tunnel-server", "-addr", ":8081", "-phone-path", "/phone"]
