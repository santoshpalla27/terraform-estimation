# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o terracost ./cmd/terracost

# Runtime Stage
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies (CA certificates for AWS API)
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /app/terracost .

# Copy migrations
COPY db/migrations /app/migrations

# Create backup directory
RUN mkdir -p /app/backups

# Expose port (if web server is added later)
EXPOSE 8080

# Environment defaults
ENV BACKUP_DIR=/app/backups

# Run the application
ENTRYPOINT ["./terracost"]
