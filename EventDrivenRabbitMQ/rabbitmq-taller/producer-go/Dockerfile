FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod .
RUN go mod download

COPY main.go .
RUN go mod tidy && go build -o producer main.go

# ---- runtime ----
FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/producer .

CMD ["./producer"]
