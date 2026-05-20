# Default target when running bare `make`
.DEFAULT_GOAL := help

# Define the root directory of the project (resolves correctly even with make -f)
LIB_STREAMING := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Include shared color definitions and utility functions
include $(LIB_STREAMING)/scripts/makefile_colors.mk
include $(LIB_STREAMING)/scripts/makefile_utils.mk

# Define common utility functions
define print_title
	@echo ""
	@echo "------------------------------------------"
	@echo "   📝 $(1)  "
	@echo "------------------------------------------"
endef

# ------------------------------------------------------
# Test configuration for lib-streaming
# ------------------------------------------------------

# Integration test filter
# RUN: specific test name pattern (e.g., TestIntegration_FeatureName)
# PKG: specific package to test (e.g., ./...)
# Usage: make test-integration RUN=TestIntegration_FeatureName
#        make test-integration PKG=./...
RUN ?=
PKG ?=

# Computed run pattern: uses RUN if set, otherwise defaults to '^TestIntegration'
# for integration runs and '^TestChaos' for chaos runs. The two profiles share
# the RUN override so callers can scope a single test across either suite.
ifeq ($(RUN),)
  RUN_PATTERN := ^TestIntegration
  CHAOS_RUN_PATTERN := ^TestChaos
else
  RUN_PATTERN := $(RUN)
  CHAOS_RUN_PATTERN := $(RUN)
endif

# Low-resource mode for limited machines (sets -p=1 -parallel=1, disables -race)
# Usage: make test LOW_RESOURCE=1
#        make test-unit LOW_RESOURCE=1
#        make test-integration LOW_RESOURCE=1
#        make coverage-unit LOW_RESOURCE=1
#        make coverage-integration LOW_RESOURCE=1
LOW_RESOURCE ?= 0
ALLOW_LOW_RESOURCE_CI ?= 0

ifeq ($(CI),true)
  ifeq ($(LOW_RESOURCE),1)
    ifneq ($(ALLOW_LOW_RESOURCE_CI),1)
      $(error LOW_RESOURCE=1 is blocked in CI because it disables race detection; set ALLOW_LOW_RESOURCE_CI=1 only for an explicit constrained-run exception)
    endif
  endif
endif

# Computed flags for low-resource mode
ifeq ($(LOW_RESOURCE),1)
  LOW_RES_P_FLAG := -p 1
  LOW_RES_PARALLEL_FLAG := -parallel 1
  LOW_RES_RACE_FLAG :=
else
  LOW_RES_P_FLAG :=
  LOW_RES_PARALLEL_FLAG :=
  LOW_RES_RACE_FLAG := -race
endif

# macOS ld64 workaround: newer ld emits noisy LC_DYSYMTAB warnings when linking test binaries with -race.
# If available, prefer Apple's classic linker to silence them.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  # Prefer classic mode to suppress LC_DYSYMTAB warnings on macOS.
  # Set DISABLE_OSX_LINKER_WORKAROUND=1 to disable this behavior.
  ifneq ($(DISABLE_OSX_LINKER_WORKAROUND),1)
    GO_TEST_LDFLAGS := -ldflags="-linkmode=external -extldflags=-ld_classic"
  else
    GO_TEST_LDFLAGS :=
  endif
else
  GO_TEST_LDFLAGS :=
endif

# ------------------------------------------------------
# Test tooling configuration
# ------------------------------------------------------

# Pinned tool versions for reproducibility (update as needed)
GOTESTSUM_VERSION ?= v1.13.0
GOSEC_VERSION ?= v2.26.1
GOLANGCI_LINT_VERSION ?= v2.12.2
GORELEASER_VERSION ?= v2.15.4
ENABLE_PERFSPRINT ?= 0

TEST_REPORTS_DIR ?= ./reports
GOTESTSUM = $(shell command -v gotestsum 2>/dev/null)
RETRY_ON_FAIL ?= 0

.PHONY: tools tools-gotestsum
tools: tools-gotestsum ## Install helpful dev/test tools

tools-gotestsum:
	@if [ -z "$(GOTESTSUM)" ]; then \
		echo "Installing gotestsum..."; \
		GO111MODULE=on go install gotest.tools/gotestsum@$(GOTESTSUM_VERSION); \
	else \
		echo "gotestsum already installed: $(GOTESTSUM)"; \
	fi

#-------------------------------------------------------
# Help Command
#-------------------------------------------------------

.PHONY: help
help:
	@echo ""
	@echo ""
	@echo "Lib-Streaming Project Management Commands"
	@echo ""
	@echo ""
	@echo "Core Commands:"
	@echo "  make help                        - Display this help message"
	@echo "  make test                        - Run unit tests (without integration)"
	@echo "  make ci                          - Run the local fix + verify pipeline"
	@echo "  make build                       - Build all packages"
	@echo "  make clean                       - Clean all build artifacts"
	@echo ""
	@echo ""
	@echo "Test Suite Commands:"
	@echo "  make test-unit                   - Run unit tests (LOW_RESOURCE=1 supported)"
	@echo "  make test-integration            - Run integration tests with testcontainers (RUN=<test>, LOW_RESOURCE=1)"
	@echo "  make test-chaos                  - Run chaos tests with toxiproxy (RUN=<test>, LOW_RESOURCE=1; sets CHAOS=1 automatically)"
	@echo "  make test-all                    - Run all tests (unit + integration + chaos)"
	@echo ""
	@echo ""
	@echo "Coverage Commands:"
	@echo "  make coverage-unit               - Run unit tests with coverage report (PKG=./path, uses .ignorecoverunit)"
	@echo "  make coverage-integration        - Run integration tests with coverage report (PKG=./path)"
	@echo "  make coverage                    - Run all coverage targets (unit + integration)"
	@echo ""
	@echo ""
	@echo "Test Tooling:"
	@echo "  make tools                       - Install test tools (gotestsum)"
	@echo ""
	@echo ""
	@echo "Code Quality Commands:"
	@echo "  make lint                        - Run linting on all packages (read-only check)"
	@echo "  make lint-fix                    - Run linting with auto-fix on all packages"
	@echo "  make format                      - Format code in all packages"
	@echo "  make tidy                        - Clean dependencies"
	@echo "  make check-tests                 - Verify tagged test package discovery"
	@echo "  make vet                         - Run go vet on all packages"
	@echo "  make sec                         - Run security checks using gosec"
	@echo "  make sec SARIF=1                 - Run security checks with SARIF output"
	@echo ""
	@echo ""
	@echo "Git Hook Commands:"
	@echo "  make setup-git-hooks             - Install and configure git hooks"
	@echo "  make check-hooks                 - Verify git hooks installation status"
	@echo "  make check-envs                  - Check if github hooks are installed and secret env files are not exposed"
	@echo ""
	@echo ""
	@echo "Release Commands:"
	@echo "  make goreleaser                  - Create release snapshot with goreleaser"
	@echo ""
	@echo ""

#-------------------------------------------------------
# Core Commands
#-------------------------------------------------------


.PHONY: build
build:
	$(call print_title,Building all packages)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	go build ./...
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All packages built successfully$(GREEN) ✔️$(NC)"

.PHONY: clean
clean:
	$(call print_title,Cleaning build artifacts)
	@rm -rf ./bin ./dist $(TEST_REPORTS_DIR) coverage.out coverage.html gosec-report.sarif
	@go clean -cache -testcache
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All build artifacts cleaned$(GREEN) ✔️$(NC)"

.PHONY: ci
ci:
	$(call print_title,Running local CI preflight pipeline)
	@printf "This target normalizes the worktree before verification.\n"
	$(MAKE) lint-fix
	$(MAKE) format
	$(MAKE) tidy
	$(MAKE) check-tests
	$(MAKE) sec
	$(MAKE) vet
	$(MAKE) test-unit
	$(MAKE) test-integration
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Local CI pipeline completed successfully$(GREEN) ✔️$(NC)"

#-------------------------------------------------------
# Core Test Commands
#-------------------------------------------------------

.PHONY: test
test:
	$(call print_title,Running all tests)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	@set -e; mkdir -p $(TEST_REPORTS_DIR); \
	if [ -n "$(GOTESTSUM)" ]; then \
	  echo "Running tests with gotestsum"; \
	  gotestsum --format testname -- -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) ./... || { \
	    if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	      echo "Retrying tests once..."; \
	      gotestsum --format testname -- -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) ./...; \
	    else \
	      exit 1; \
	    fi; \
	  }; \
	else \
	  go test -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) ./... || { \
	    if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	      echo "Retrying tests once..."; \
	      go test -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) ./...; \
	    else \
	      exit 1; \
	    fi; \
	  }; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All tests passed$(GREEN) ✔️$(NC)"

#-------------------------------------------------------
# Test Suite Aliases
#-------------------------------------------------------

# Unit tests (excluding integration tests)
.PHONY: test-unit
test-unit:
	$(call print_title,Running Go unit tests)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	@set -e; mkdir -p $(TEST_REPORTS_DIR); \
	pkgs=$$(go list -tags=unit ./...); \
	if [ -z "$$pkgs" ]; then \
	  echo "No unit test packages found"; \
	else \
	  if [ -n "$(GOTESTSUM)" ]; then \
	    echo "Running unit tests with gotestsum"; \
	    gotestsum --format testname -- -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying unit tests once..."; \
	        gotestsum --format testname -- -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  else \
	    go test -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying unit tests once..."; \
	        go test -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  fi; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Unit tests passed$(GREEN) ✔️$(NC)"

# Integration tests with testcontainers (no coverage)
# These tests use the `integration` build tag and testcontainers-go to spin up
# ephemeral containers. No external Docker stack is required.
#
# Requirements:
#   - Test files must follow the naming convention: *_integration_test.go
#   - Test functions must start with TestIntegration_ (e.g., TestIntegration_MyFeature_Works)
.PHONY: test-integration
test-integration:
	$(call print_title,Running integration tests with testcontainers)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	$(call check_command,docker,"Install Docker from https://docs.docker.com/get-docker/")
	@set -e; mkdir -p $(TEST_REPORTS_DIR); \
	if [ -n "$(PKG)" ]; then \
	  echo "Using specified package: $(PKG)"; \
	  pkgs=$$(go list $(PKG) 2>/dev/null | tr '\n' ' '); \
	else \
	  echo "Finding packages with *_integration_test.go files..."; \
	  dirs=$$(find . -name '*_integration_test.go' -not -path './vendor/*' -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ' '); \
	  pkgs=$$(if [ -n "$$dirs" ]; then go list $$dirs 2>/dev/null | tr '\n' ' '; fi); \
	fi; \
	if [ -z "$$pkgs" ]; then \
	  echo "No integration test packages found"; \
	  if [ "$(ALLOW_NO_INTEGRATION_TESTS)" != "1" ]; then \
	    echo "Set ALLOW_NO_INTEGRATION_TESTS=1 to acknowledge an intentional integration-free repository"; \
	    exit 1; \
	  fi; \
	else \
	  echo "Packages: $$pkgs"; \
	  echo "Running packages sequentially (-p=1) to avoid Docker container conflicts"; \
	  if [ "$(LOW_RESOURCE)" = "1" ]; then \
	    echo "LOW_RESOURCE mode: -parallel=1, race detector disabled"; \
	  fi; \
	  if [ -n "$(GOTESTSUM)" ]; then \
	    echo "Running integration tests with gotestsum"; \
	    gotestsum --format testname -- \
	      -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	      -p 1 $(LOW_RES_PARALLEL_FLAG) \
	      -run '$(RUN_PATTERN)' $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying integration tests once..."; \
	        gotestsum --format testname -- \
	          -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	          -p 1 $(LOW_RES_PARALLEL_FLAG) \
	          -run '$(RUN_PATTERN)' $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  else \
	    go test -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	      -p 1 $(LOW_RES_PARALLEL_FLAG) \
	      -run '$(RUN_PATTERN)' $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying integration tests once..."; \
	        go test -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	          -p 1 $(LOW_RES_PARALLEL_FLAG) \
	          -run '$(RUN_PATTERN)' $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  fi; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Integration tests passed$(GREEN) ✔️$(NC)"

# Chaos tests with toxiproxy (no coverage)
# These tests use the `chaos` build tag and testcontainers-go to spin up an
# ephemeral Redpanda + Toxiproxy pair, then inject network faults (latency,
# partition, jitter) to verify graceful degradation: circuit-breaker trip,
# outbox-fallback absorption, and recovery on toxic removal.
#
# The chaos suite is dual-gated by design: -tags=chaos selects the build, and
# CHAOS=1 in the env turns the tests on (otherwise they t.Skip cleanly so a
# bare `go test ./...` against the chaos tag does not consume Docker hours
# unintentionally). This target sets CHAOS=1 automatically — callers do not
# need to remember it.
#
# Requirements:
#   - Test files must follow the naming convention: *chaos_test.go
#   - Test functions must start with TestChaos_ (e.g., TestChaos_BrokerLatency)
#   - Docker required for both Redpanda and Toxiproxy testcontainers
.PHONY: test-chaos
test-chaos:
	$(call print_title,Running chaos tests with toxiproxy)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	$(call check_command,docker,"Install Docker from https://docs.docker.com/get-docker/")
	@set -e; mkdir -p $(TEST_REPORTS_DIR); \
	if [ -n "$(PKG)" ]; then \
	  echo "Using specified package: $(PKG)"; \
	  pkgs=$$(go list $(PKG) 2>/dev/null | tr '\n' ' '); \
	else \
	  echo "Finding packages with *chaos_test.go files..."; \
	  dirs=$$(find . -name '*chaos_test.go' -not -path './vendor/*' -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ' '); \
	  pkgs=$$(if [ -n "$$dirs" ]; then go list $$dirs 2>/dev/null | tr '\n' ' '; fi); \
	fi; \
	if [ -z "$$pkgs" ]; then \
	  echo "No chaos test packages found"; \
	  if [ "$(ALLOW_NO_CHAOS_TESTS)" != "1" ]; then \
	    echo "Set ALLOW_NO_CHAOS_TESTS=1 to acknowledge an intentional chaos-free repository"; \
	    exit 1; \
	  fi; \
	else \
	  echo "Packages: $$pkgs"; \
	  echo "Running packages sequentially (-p=1) to avoid Docker container conflicts"; \
	  echo "CHAOS=1 set automatically by Makefile target"; \
	  if [ "$(LOW_RESOURCE)" = "1" ]; then \
	    echo "LOW_RESOURCE mode: -parallel=1, race detector disabled"; \
	  fi; \
	  if [ -n "$(GOTESTSUM)" ]; then \
	    echo "Running chaos tests with gotestsum"; \
	    CHAOS=1 gotestsum --format testname -- \
	      -tags=chaos -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	      -p 1 $(LOW_RES_PARALLEL_FLAG) \
	      -run '$(CHAOS_RUN_PATTERN)' $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying chaos tests once..."; \
	        CHAOS=1 gotestsum --format testname -- \
	          -tags=chaos -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	          -p 1 $(LOW_RES_PARALLEL_FLAG) \
	          -run '$(CHAOS_RUN_PATTERN)' $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  else \
	    CHAOS=1 go test -tags=chaos -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	      -p 1 $(LOW_RES_PARALLEL_FLAG) \
	      -run '$(CHAOS_RUN_PATTERN)' $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying chaos tests once..."; \
	        CHAOS=1 go test -tags=chaos -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	          -p 1 $(LOW_RES_PARALLEL_FLAG) \
	          -run '$(CHAOS_RUN_PATTERN)' $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  fi; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Chaos tests passed$(GREEN) ✔️$(NC)"

# Run all tests (unit + integration + chaos)
.PHONY: test-all
test-all:
	$(call print_title,Running all tests (unit + integration + chaos))
	$(call print_title,Running unit tests)
	$(MAKE) test-unit
	$(call print_title,Running integration tests)
	$(MAKE) test-integration
	$(call print_title,Running chaos tests)
	$(MAKE) test-chaos
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All tests passed$(GREEN) ✔️$(NC)"

#-------------------------------------------------------
# Coverage Commands
#-------------------------------------------------------

# Unit tests with coverage (uses covermode=atomic)
# Supports PKG parameter to filter packages (e.g., PKG=./...)
# Supports .ignorecoverunit file to exclude patterns from coverage stats
.PHONY: coverage-unit
coverage-unit:
	$(call print_title,Running Go unit tests with coverage)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	@set -e; mkdir -p $(TEST_REPORTS_DIR); \
	if [ -n "$(PKG)" ]; then \
	  echo "Using specified package: $(PKG)"; \
	  pkgs=$$(go list $(PKG) 2>/dev/null | grep -v '/tests' | tr '\n' ' '); \
	else \
	  pkgs=$$(go list ./... | grep -v '/tests'); \
	fi; \
	if [ -z "$$pkgs" ]; then \
	  echo "No unit test packages found"; \
	else \
	  echo "Packages: $$pkgs"; \
	  if [ -n "$(GOTESTSUM)" ]; then \
	    echo "Running unit tests with gotestsum (coverage enabled)"; \
	    gotestsum --format testname -- -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/unit_coverage.out $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying unit tests once..."; \
	        gotestsum --format testname -- -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/unit_coverage.out $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  else \
	    go test -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/unit_coverage.out $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying unit tests once..."; \
	        go test -tags=unit -v $(LOW_RES_P_FLAG) $(LOW_RES_RACE_FLAG) $(LOW_RES_PARALLEL_FLAG) -count=1 $(GO_TEST_LDFLAGS) -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/unit_coverage.out $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  fi; \
	  if [ -f .ignorecoverunit ]; then \
	    echo "Filtering coverage with .ignorecoverunit patterns..."; \
	    patterns=$$(grep -v '^#' .ignorecoverunit | grep -v '^$$' | tr '\n' '|' | sed 's/|$$//'); \
	    if [ -n "$$patterns" ]; then \
	      regex_patterns=$$(echo "$$patterns" | sed 's/[][(){}+?^$$\\|]/\\&/g' | sed 's/\./\\./g' | sed 's/\*/.*/g'); \
	      head -1 $(TEST_REPORTS_DIR)/unit_coverage.out > $(TEST_REPORTS_DIR)/unit_coverage_filtered.out; \
	      tail -n +2 $(TEST_REPORTS_DIR)/unit_coverage.out | grep -vE "$$regex_patterns" >> $(TEST_REPORTS_DIR)/unit_coverage_filtered.out || true; \
	      mv $(TEST_REPORTS_DIR)/unit_coverage_filtered.out $(TEST_REPORTS_DIR)/unit_coverage.out; \
	      echo "Excluded patterns: $$patterns"; \
	    fi; \
	  fi; \
	  echo "----------------------------------------"; \
	  go tool cover -func=$(TEST_REPORTS_DIR)/unit_coverage.out | grep total | awk '{print "Total coverage: " $$3}'; \
	  echo "----------------------------------------"; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Unit coverage report generated$(GREEN) ✔️$(NC)"

# Integration tests with testcontainers (with coverage, uses covermode=atomic)
.PHONY: coverage-integration
coverage-integration:
	$(call print_title,Running integration tests with testcontainers (coverage enabled))
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	$(call check_command,docker,"Install Docker from https://docs.docker.com/get-docker/")
	@set -e; mkdir -p $(TEST_REPORTS_DIR); \
	if [ -n "$(PKG)" ]; then \
	  echo "Using specified package: $(PKG)"; \
	  pkgs=$$(go list $(PKG) 2>/dev/null | tr '\n' ' '); \
	else \
	  echo "Finding packages with *_integration_test.go files..."; \
	  dirs=$$(find . -name '*_integration_test.go' -not -path './vendor/*' -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ' '); \
	  pkgs=$$(if [ -n "$$dirs" ]; then go list $$dirs 2>/dev/null | tr '\n' ' '; fi); \
	fi; \
	if [ -z "$$pkgs" ]; then \
	  echo "No integration test packages found"; \
	  if [ "$(ALLOW_NO_INTEGRATION_TESTS)" != "1" ]; then \
	    echo "Set ALLOW_NO_INTEGRATION_TESTS=1 to acknowledge an intentional integration-free repository"; \
	    exit 1; \
	  fi; \
	else \
	  echo "Packages: $$pkgs"; \
	  echo "Running packages sequentially (-p=1) to avoid Docker container conflicts"; \
	  if [ "$(LOW_RESOURCE)" = "1" ]; then \
	    echo "LOW_RESOURCE mode: -parallel=1, race detector disabled"; \
	  fi; \
	  if [ -n "$(GOTESTSUM)" ]; then \
	    echo "Running testcontainers integration tests with gotestsum (coverage enabled)"; \
	    gotestsum --format testname -- \
	      -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	      -p 1 $(LOW_RES_PARALLEL_FLAG) \
	      -run '$(RUN_PATTERN)' -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/integration_coverage.out \
	      $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying integration tests once..."; \
	        gotestsum --format testname -- \
	          -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	          -p 1 $(LOW_RES_PARALLEL_FLAG) \
	          -run '$(RUN_PATTERN)' -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/integration_coverage.out \
	          $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  else \
	    go test -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	      -p 1 $(LOW_RES_PARALLEL_FLAG) \
	      -run '$(RUN_PATTERN)' -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/integration_coverage.out \
	      $$pkgs || { \
	      if [ "$(RETRY_ON_FAIL)" = "1" ]; then \
	        echo "Retrying integration tests once..."; \
	        go test -tags=integration -v $(LOW_RES_RACE_FLAG) -count=1 -timeout 600s $(GO_TEST_LDFLAGS) \
	          -p 1 $(LOW_RES_PARALLEL_FLAG) \
	          -run '$(RUN_PATTERN)' -covermode=atomic -coverprofile=$(TEST_REPORTS_DIR)/integration_coverage.out \
	          $$pkgs; \
	      else \
	        exit 1; \
	      fi; \
	    }; \
	  fi; \
	  echo "----------------------------------------"; \
	  go tool cover -func=$(TEST_REPORTS_DIR)/integration_coverage.out | grep total | awk '{print "Total coverage: " $$3}'; \
	  echo "----------------------------------------"; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Integration coverage report generated$(GREEN) ✔️$(NC)"

# Run all coverage targets
.PHONY: coverage
coverage:
	$(call print_title,Running all coverage targets)
	$(MAKE) coverage-unit
	$(MAKE) coverage-integration
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All coverage reports generated$(GREEN) ✔️$(NC)"

#-------------------------------------------------------
# Code Quality Commands
#-------------------------------------------------------

.PHONY: lint
lint:
	$(call print_title,Running linters on all packages (read-only))
	$(call check_command,golangci-lint,"go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)")
	@out=$$(golangci-lint run ./... 2>&1); \
	out_err=$$?; \
	if [ "$(ENABLE_PERFSPRINT)" = "1" ]; then \
	  if ! command -v perfsprint >/dev/null 2>&1; then \
	    echo "ENABLE_PERFSPRINT=1 requires perfsprint on PATH"; \
	    exit 1; \
	  fi; \
		perf_out=$$(perfsprint ./... 2>&1); \
		perf_err=$$?; \
	else \
		perf_out=""; \
		perf_err=0; \
		echo "Optional standalone perfsprint check disabled; set ENABLE_PERFSPRINT=1 to require it"; \
	fi; \
	echo "$$out"; \
	if [ -n "$$perf_out" ]; then echo "$$perf_out"; fi; \
	if [ $$out_err -ne 0 ]; then \
		printf "\n%s\n" "$(BOLD)$(RED)An error has occurred during the lint process:$(NC)"; \
		printf "%s\n" "$$out"; \
		exit 1; \
	fi; \
	if [ $$perf_err -ne 0 ]; then \
		printf "\n%s\n" "$(BOLD)$(RED)An error has occurred during the performance check:$(NC)"; \
		printf "%s\n" "$$perf_out"; \
		exit 1; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Lint and performance checks passed successfully$(GREEN) ✔️$(NC)"

.PHONY: lint-fix
lint-fix:
	$(call print_title,Running linters with auto-fix on all packages)
	$(call check_command,golangci-lint,"go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)")
	@golangci-lint run --fix ./...
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Lint auto-fix completed$(GREEN) ✔️$(NC)"

.PHONY: format
format:
	$(call print_title,Formatting code in all packages)
	$(call check_command,gofmt,"Install Go from https://golang.org/doc/install")
	@gofmt -w ./
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All go files formatted$(GREEN) ✔️$(NC)"

.PHONY: check-tests
check-tests:
	$(call print_title,Verifying tagged test packages and unit coverage)
	@if [ -f "./scripts/check-tests.sh" ]; then \
		sh ./scripts/check-tests.sh; \
	else \
		echo "scripts/check-tests.sh not found; running built-in unit coverage and tag-aware test checks"; \
		$(MAKE) coverage-unit; \
		unit_pkgs=$$(go list -tags=unit ./... | tr '\n' ' '); \
		if [ -z "$$unit_pkgs" ]; then echo "No unit packages found under -tags=unit"; exit 1; fi; \
		go test -tags=unit -run '^$$' $$unit_pkgs; \
		integration_dirs=$$(find . -name '*_integration_test.go' -not -path './vendor/*' -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ' '); \
		if [ -n "$$integration_dirs" ]; then \
			integration_pkgs=$$(go list $$integration_dirs 2>/dev/null | tr '\n' ' '); \
			if [ -z "$$integration_pkgs" ]; then echo "Integration test files found but no packages resolved"; exit 1; fi; \
			go test -tags=integration -run '^$$' $$integration_pkgs; \
		else \
			echo "No integration test files found during tag-aware check"; \
		fi; \
		chaos_dirs=$$(find . -name '*chaos_test.go' -not -path './vendor/*' -exec dirname {} \; 2>/dev/null | sort -u | tr '\n' ' '); \
		if [ -n "$$chaos_dirs" ]; then \
			chaos_pkgs=$$(go list $$chaos_dirs 2>/dev/null | tr '\n' ' '); \
			if [ -z "$$chaos_pkgs" ]; then echo "Chaos test files found but no packages resolved"; exit 1; fi; \
			go test -tags=chaos -run '^$$' $$chaos_pkgs; \
		else \
			echo "No chaos test files found during tag-aware check"; \
		fi; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Tagged test package verification completed$(GREEN) ✔️$(NC)"

.PHONY: vet
vet:
	$(call print_title,Running go vet on all packages)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	go vet ./...
	@echo "$(GREEN)$(BOLD)[ok]$(NC) go vet completed successfully$(GREEN) ✔️$(NC)"

#-------------------------------------------------------
# Git Hook Commands
#-------------------------------------------------------

.PHONY: setup-git-hooks
setup-git-hooks:
	$(call print_title,Installing and configuring git hooks)
	@hooks_dir=$$(git rev-parse --git-path hooks); \
	if [ ! -d .githooks ]; then \
		echo "No .githooks directory found, skipping"; \
		exit 0; \
	fi; \
	mkdir -p "$$hooks_dir"; \
	for hook_dir in .githooks/*/; do \
		if [ -d "$$hook_dir" ]; then \
			for FILE in "$$hook_dir"*; do \
				if [ -f "$$FILE" ]; then \
					hook_name=$$(basename "$$FILE"); \
					cp "$$FILE" "$$hooks_dir/$$hook_name"; \
					chmod +x "$$hooks_dir/$$hook_name"; \
				fi; \
			done; \
		fi; \
	done
	@echo "$(GREEN)$(BOLD)[ok]$(NC) All hooks installed and updated$(GREEN) ✔️$(NC)"

.PHONY: check-hooks
check-hooks:
	$(call print_title,Verifying git hooks installation status)
	@hooks_dir=$$(git rev-parse --git-path hooks); \
	err=0; \
	for hook_dir in .githooks/*; do \
		if [ -d "$$hook_dir" ]; then \
			for FILE in "$$hook_dir"/*; do \
				if [ -f "$$FILE" ]; then \
					f=$$(basename -- $$hook_dir)/$$(basename -- $$FILE); \
					hook_name=$$(basename -- $$FILE); \
					FILE2=$$hooks_dir/$$hook_name; \
					if [ -f "$$FILE2" ]; then \
						if cmp -s "$$FILE" "$$FILE2"; then \
							echo "$(GREEN)$(BOLD)[ok]$(NC) Hook file $$f installed and updated$(GREEN) ✔️$(NC)"; \
						else \
							echo "$(RED)Hook file $$f installed but out-of-date [OUT-OF-DATE] ✗$(NC)"; \
							err=1; \
						fi; \
					else \
						echo "$(RED)Hook file $$f not installed [NOT INSTALLED] ✗$(NC)"; \
						err=1; \
					fi; \
				fi; \
			done; \
		fi; \
	done; \
	if [ $$err -ne 0 ]; then \
		printf "\nRun %smake setup-git-hooks%s to setup your development environment, then try again.\n\n" "$(BOLD)" "$(NC)"; \
		exit 1; \
	else \
		echo "$(GREEN)$(BOLD)[ok]$(NC) All hooks are properly installed$(GREEN) ✔️$(NC)"; \
	fi

.PHONY: check-envs
check-envs:
	$(call print_title,Checking git hooks and environment files for security issues)
	$(MAKE) check-hooks
	@echo "Checking for exposed secrets in environment files..."
	@found=0; \
	for pattern in '.env' '.env.*' '*.env'; do \
		files=$$(find . -name "$$pattern" \
			-not -name '*.example' -not -name '*.sample' -not -name '*.template' \
			-not -path './vendor/*' -not -path './.git/*' 2>/dev/null); \
		if [ -n "$$files" ]; then \
			if echo "$$files" | xargs grep -iqE '^[[:space:]]*(export[[:space:]]+)?[A-Z0-9_]*(SECRET|PASSWORD|TOKEN|API_KEY|PRIVATE_KEY|CREDENTIAL|AWS_ACCESS_KEY|DB_PASS)[A-Z0-9_]*=[[:space:]]*[^#[:space:]]' 2>/dev/null; then \
				echo "$(RED)Warning: Potential secrets found in environment files:$(NC)"; \
				echo "$$files" | xargs grep -ilE '^[[:space:]]*(export[[:space:]]+)?[A-Z0-9_]*(SECRET|PASSWORD|TOKEN|API_KEY|PRIVATE_KEY|CREDENTIAL|AWS_ACCESS_KEY|DB_PASS)[A-Z0-9_]*=[[:space:]]*[^#[:space:]]' 2>/dev/null; \
				found=1; \
			fi; \
		fi; \
	done; \
	if [ $$found -ne 0 ]; then \
		echo "$(RED)Make sure these files are in .gitignore and not committed to the repository.$(NC)"; \
		exit 1; \
	else \
		echo "$(GREEN)No exposed secrets found in environment files$(GREEN) ✔️$(NC)"; \
	fi
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Environment check completed$(GREEN) ✔️$(NC)"

#-------------------------------------------------------
# Development Commands
#-------------------------------------------------------

.PHONY: tidy
tidy:
	$(call print_title,Cleaning dependencies)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	go mod tidy
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Dependencies cleaned successfully$(GREEN) ✔️$(NC)"

# SARIF output for GitHub Security tab integration (optional)
# Usage: make sec SARIF=1
SARIF ?= 0

.PHONY: sec
sec:
	$(call print_title,Running security checks using gosec)
	@if ! command -v gosec >/dev/null 2>&1; then \
		echo "Installing gosec..."; \
		go install github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION); \
	fi
	@if find . -name "*.go" -type f -not -path './vendor/*' | grep -q .; then \
		echo "Running security checks on all packages..."; \
		if [ "$(SARIF)" = "1" ]; then \
			echo "Generating SARIF output: gosec-report.sarif"; \
			if gosec -fmt sarif -out gosec-report.sarif ./...; then \
				echo "$(GREEN)$(BOLD)[ok]$(NC) SARIF report generated: gosec-report.sarif$(GREEN) ✔️$(NC)"; \
			else \
				printf "\n%s%sSecurity issues found by gosec. Please address them before proceeding.%s\n\n" "$(BOLD)" "$(RED)" "$(NC)"; \
				echo "SARIF report with details: gosec-report.sarif"; \
				exit 1; \
			fi; \
		else \
			if gosec ./...; then \
				echo "$(GREEN)$(BOLD)[ok]$(NC) Security checks completed$(GREEN) ✔️$(NC)"; \
			else \
				printf "\n%s%sSecurity issues found by gosec. Please address them before proceeding.%s\n\n" "$(BOLD)" "$(RED)" "$(NC)"; \
				exit 1; \
			fi; \
		fi; \
	else \
		echo "No Go files found, skipping security checks"; \
	fi

#-------------------------------------------------------
# Release Commands
#-------------------------------------------------------

.PHONY: goreleaser
goreleaser:
	$(call print_title,Creating release snapshot with goreleaser)
	$(call check_command,go,"Install Go from https://golang.org/doc/install")
	go run github.com/goreleaser/goreleaser/v2@$(GORELEASER_VERSION) release --snapshot --skip=publish --clean
	@echo "$(GREEN)$(BOLD)[ok]$(NC) Release snapshot created successfully$(GREEN) ✔️$(NC)"
