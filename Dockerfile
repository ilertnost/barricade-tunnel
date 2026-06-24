FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY tunnel-server /tunnel-server
EXPOSE 8081
CMD ["/tunnel-server", "-addr", ":8081"]
