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
	go build -o $(BIN)/orion-bench cmd/orion-bench/main.go

proto:
	protoc --go_out=. protos/*.proto
