BIN = $(CURDIR)/bin

.DEFAULT_GOAL := binary

$(BIN):
	@mkdir -p $@

.PHONY: binary
binary:
	go build -o $(BIN)/orion-bench cmd/orion-bench/main.go

.PHONY: clean
clean:
	rm -rf $(BIN)
