.DEFAULT_GOAL := help

GO          ?= go
GOBIN       := $(shell $(GO) env GOPATH)/bin
MODULE      := github.com/emmanueladutwum123/quorumkv
BUILD_DIR   := bin
PROTO_FILES := $(shell find proto -name '*.proto')

# Pinned so that regenerating protobuf code on a different machine produces a
# byte-identical result. An unpinned plugin turns a routine `make proto` into a
# diff full of unrelated churn.
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the server and CLI into bin/
	@mkdir -p $(BUILD_DIR)
	$(GO) build -o $(BUILD_DIR)/ ./cmd/...

.PHONY: test
test: ## Run the unit and integration test suites
	$(GO) test ./...

.PHONY: race
race: ## Run tests under the race detector
	$(GO) test -race ./...

# The consensus core is the part where a missed branch is a correctness bug
# rather than a cosmetic gap, so its coverage is reported separately from the
# whole-module figure that plumbing and generated code would dilute.
.PHONY: cover
cover: ## Run tests with coverage and report the consensus core separately
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	@$(GO) tool cover -func=coverage.out | tail -1
	@echo "--- consensus core ---"
	@$(GO) test -coverprofile=core.out -covermode=atomic ./internal/raft/ >/dev/null
	@$(GO) tool cover -func=core.out | tail -1

.PHONY: cover-html
cover-html: cover ## Open the coverage report in a browser
	$(GO) tool cover -html=coverage.out

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format all Go sources
	$(GO) fmt ./...

.PHONY: lint
lint: ## Verify formatting, vet, and module tidiness
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './internal/gen/*'))" \
		|| { echo "gofmt found unformatted files:"; gofmt -l $$(find . -name '*.go' -not -path './internal/gen/*'); exit 1; }
	$(GO) vet ./...
	@cp go.mod go.mod.bak && cp go.sum go.sum.bak
	@$(GO) mod tidy
	@diff -q go.mod go.mod.bak >/dev/null && diff -q go.sum go.sum.bak >/dev/null \
		|| { echo "go.mod/go.sum are not tidy; run 'go mod tidy'"; mv go.mod.bak go.mod; mv go.sum.bak go.sum; exit 1; }
	@rm -f go.mod.bak go.sum.bak

.PHONY: tools
tools: ## Install the pinned protobuf plugins
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: proto
proto: ## Regenerate Go code from the .proto contracts
	PATH="$(PATH):$(GOBIN)" protoc \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

.PHONY: clean
clean: ## Remove build and coverage artefacts
	rm -rf $(BUILD_DIR) coverage.out core.out
