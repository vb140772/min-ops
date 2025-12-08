PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)

TARGET_GOARCH ?= $(shell go env GOARCH)
TARGET_GOOS ?= $(shell go env GOOS)

BINARY_NAME := mdb

.PHONY: all build clean install checks

all: build

checks:
	@echo "Checking dependencies"
	@go mod verify
	@go mod tidy

# Version information
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Builds mdb locally.
build: checks
	@echo "Building $(BINARY_NAME) binary to './$(BINARY_NAME)'"
	@echo "Version: $(VERSION), Commit: $(COMMIT), BuildDate: $(BUILD_DATE)"
	@GO111MODULE=on GOOS=$(TARGET_GOOS) GOARCH=$(TARGET_GOARCH) CGO_ENABLED=0 go build -trimpath \
		-ldflags "-X 'main.Version=$(VERSION)' -X 'main.Commit=$(COMMIT)' -X 'main.BuildDate=$(BUILD_DATE)'" \
		-o $(PWD)/$(BINARY_NAME)

# Builds mdb and installs it to $GOPATH/bin.
install: build
	@echo "Installing $(BINARY_NAME) binary to '$(GOPATH)/bin/$(BINARY_NAME)'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/$(BINARY_NAME) $(GOPATH)/bin/$(BINARY_NAME)
	@echo "Installation successful. To learn more, try \"$(BINARY_NAME) --help\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf $(BINARY_NAME)
	@rm -rvf dist/

# GoReleaser targets
.PHONY: release release-build release-test release-snapshot release-validate

# Test GoReleaser configuration
release-test:
	@echo "Testing GoReleaser configuration"
	@if ! command -v goreleaser >/dev/null 2>&1; then \
		echo "GoReleaser not found. Install with: go install github.com/goreleaser/goreleaser/v2@latest"; \
		exit 1; \
	fi
	@goreleaser check

# Build release (without publishing)
release-build: release-test
	@echo "Building release artifacts"
	@goreleaser build --snapshot --clean

# Create snapshot release (local, no git tag required)
release-snapshot: release-test
	@echo "Creating snapshot release"
	@goreleaser release --snapshot --clean

# Validate GoReleaser configuration
release-validate: release-test
	@goreleaser check

# Create full release (requires git tag and GITHUB_TOKEN)
release: release-test
	@echo "Creating full release"
	@if [ -z "$$GITHUB_TOKEN" ]; then \
		echo "Error: GITHUB_TOKEN environment variable is required"; \
		exit 1; \
	fi
	@goreleaser release --clean

# Create release with next version (helper script)
release-next:
	@echo "Creating release with next version"
	@read -p "Enter version (e.g., v1.0.0): " version; \
	git tag -a $$version -m "Release $$version"; \
	git push origin $$version; \
	make release

