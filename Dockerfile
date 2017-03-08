# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
FROM golang:alpine

# gcc for cgo
RUN apt-get update && apt-get install -y \
    gcc curl git libc6-dev make ca-certificates librados-dev \
    --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" "$GOPATH/src/imaginary" && chmod -R 777 "$GOPATH"
WORKDIR $GOPATH

WORKDIR $GOPATH/src/imaginary-backup-consumer
ADD . ./
RUN go get  ./...

# Run the outyet command by default when the container starts.
ENTRYPOINT ["/go/bin/imaginary-backup-consumer"]
