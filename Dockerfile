# Use a lightweight Linux distribution as the base image
FROM golang:1.21-alpine AS builder
# Set the working directory inside the container
WORKDIR /build

# Copy the Go module files
COPY go.mod go.sum ./

# Download the Go module dependencies
RUN go mod download

# Copy the entire application source
COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o iac-mqtt-linux

# Final stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /build/iac-mqtt-linux /app/iac-mqtt-linux
COPY --from=builder /build/mqttconfig.json /app/mqttconfig.json
COPY --from=builder /build/dockerconfiguration.json /app/configuration.json

# Set permissions on the application (if needed)
RUN chmod +x iac-mqtt-linux


# Expose additional ports
EXPOSE 8800
# Define an entry point to run the application

CMD ["./iac-mqtt-linux"]