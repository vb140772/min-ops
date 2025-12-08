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

# Builds mdb locally.
build: checks
	@echo "Building $(BINARY_NAME) binary to './$(BINARY_NAME)'"
	@GO111MODULE=on GOOS=$(TARGET_GOOS) GOARCH=$(TARGET_GOARCH) CGO_ENABLED=0 go build -trimpath -o $(PWD)/$(BINARY_NAME)

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

