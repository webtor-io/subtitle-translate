FROM alpine:3.21 AS certs
RUN apk add --no-cache ca-certificates

FROM golang:1.25-alpine AS build
WORKDIR /app
COPY . .
ENV GOOS=linux CGO_ENABLED=0
RUN go build -ldflags '-w -s' -o server

FROM alpine:3.21
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/server .
EXPOSE 8080 8081 8083
CMD ["./server"]
