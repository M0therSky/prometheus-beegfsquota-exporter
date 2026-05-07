BINARY_NAME=beegfs-quota-exporter
GO=go
CGO_ENABLED=0
LDFLAGS=-w -s

HOST=localhost
PORT=9742

.PHONY: all build run test clean

all: build

build:
	CGO_ENABLED=$(CGO_ENABLED) $(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) ./cmd/beegfs-quota-exporter

run:
	./$(BINARY_NAME) -host=$(HOST) -port=$(PORT)

test:
	$(GO) test ./...

clean:
	rm -f $(BINARY_NAME)
