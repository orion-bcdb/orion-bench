# Allow setting different go version from the command line. E.g.,`make GO=go1.19.4 binary`
GO ?= go
BIN = bin
ALL_FILES = $(shell find . -type f -name "*.go")

.DEFAULT_GOAL := binary

$(BIN):
	@mkdir -p $@

.PHONY: binary
binary: $(BIN)/orion-bench

.PHONY: clean
clean:
	rm -rf $(BIN)

$(BIN)/orion-bench: $(BIN) $(ALL_FILES)
	$(GO) build -o $(BIN)/orion-bench cmd/orion-bench/main.go
