BIN = $(CURDIR)/bin

.DEFAULT_GOAL := binary

$(BIN):
	@mkdir -p $@

.PHONY: binary
binary:
	go build -o $(BIN)/orion-bench cmd/orion-bench/main.go
	go build -o $(BIN)/bdb github.com/hyperledger-labs/orion-server/cmd/bdb

.PHONY: clean
clean:
	rm -rf $(BIN)
