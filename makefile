# Check if zsh is available, otherwise use bash
SHELL := $(shell if command -v zsh >/dev/null 2>&1; then echo zsh; else echo /bin/bash; fi)

UID:= $(shell id -u)
GID:= $(shell id -g)
MODULE?=common
APPEND?=
TEST_FILTER?=

# Load env file
ifeq ($(wildcard .env),)
  $(info The .env file doesn't not exist, some feature will be deactivated)
  SKIP_TARGETS := download_charts init start start_minikube dashboard
else
  include .env
  export $(shell sed 's/=.*//' .env)
endif

ifeq ($(wildcard .envdb),)
  $(info The .envdb file doesn't not exist, some feature will be deactivated)
  SKIP_TARGETS_DB := false
else
  include .envdb
  export $(shell sed 's/=.*//' .envdb)
endif

MINIKUBE_PROFILE := search-service
NAMESPACE := search-service

# Public Targets list
.PHONY: all init start clean delete help setup check-deps

# Default target
all: help

setup: check-deps ## Complete setup: install dependencies, prepare environment, and initialize everything
	@echo "========================================="
	@echo "Starting complete development setup..."
	@echo "========================================="
	@$(MAKE) setup-env || (echo "❌ Environment setup failed. Run: make setup-env"; exit 1)
	@$(MAKE) setup-python || (echo "❌ Python setup failed. Run: make setup-python"; exit 1)
	@echo "========================================="
	@echo "✅ Setup completed successfully!"
	@echo "You can now run: make start"
	@echo "========================================="

check-deps: ## Check if all required system dependencies are installed
	@echo "Checking system dependencies..."
	@MISSING_DEPS=""; \
	command -v minikube >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS minikube"; \
	command -v kubectl >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS kubectl"; \
	command -v helm >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS helm"; \
	command -v tilt >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS tilt"; \
	command -v python3.10 >/dev/null 2>&1 || command -v python3.12 >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS python3.10/3.12"; \
	command -v uv >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS uv"; \
	command -v git >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS git"; \
	command -v gpg >/dev/null 2>&1 || MISSING_DEPS="$$MISSING_DEPS gpg"; \
	if [ -n "$$MISSING_DEPS" ]; then \
		echo "❌ Missing system dependencies:$$MISSING_DEPS"; \
		echo "Please install the missing dependencies first:"; \
		echo ""; \
		echo "For minikube: https://minikube.sigs.k8s.io/docs/start/"; \
		echo "For kubectl: https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/"; \
		echo "For helm: curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && chmod 700 get_helm.sh && ./get_helm.sh"; \
		echo "For tilt: curl -fsSL https://raw.githubusercontent.com/tilt-dev/tilt/master/scripts/install.sh | bash"; \
		echo "For python: sudo apt install python3.10 python3.10-venv"; \
		echo "For uv: curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		echo ""; \
		exit 1; \
	else \
		echo "✅ All system dependencies are installed"; \
	fi

setup-env: ## Setup environment files
	@echo "Setting up environment..."
	@if [ ! -f .env ]; then \
		if [ -f .env.gpg ]; then \
			echo "Decrypting .env file..."; \
			gpg -d .env.gpg > .env || (echo "❌ Failed to decrypt .env.gpg. Please ensure you have the correct GPG key."; exit 1); \
			echo "✅ Environment file decrypted successfully"; \
		else \
			echo "❌ Neither .env nor .env.gpg found. Please ensure you have the environment files."; \
			exit 1; \
		fi; \
	else \
		echo "✅ .env file already exists"; \
	fi


setup-python: ## Setup Python virtual environment
	@echo "Setting up Python environment..."
	@source .env
	@uv sync


init: start_minikube pre_init addons ## Initialize a project and start the development environment
	@echo "Creating namespace $(NAMESPACE)..."
	@kubectl create namespace $(NAMESPACE)
	@echo "======= > Init done please next time run 'make start' < ========="
	@tilt up $(DEV_PROFILES)


pre_init:
# Folders must be present before the storage is loaded by minikube so we can't move that to the tilt script 
	@echo "Creating storages... => $(PWD)/_data"
	# You need to create all the directory before them
	@mkdir -p _data/db


addons: ## Install addons for minikube
	@echo "Setting up minikube addons..."
	@minikube addons enable ingress -p $(MINIKUBE_PROFILE)
	@minikube -p $(MINIKUBE_PROFILE) addons enable metrics-server

start_minikube: ## Start minikube with the defined parameters
	@echo "Using shell: $(SHELL)"
	@echo "Starting minikube...$(UID):$(GID)"
	@minikube start --preload=false --kubernetes-version=1.27.9 --driver=docker -p $(MINIKUBE_PROFILE) --namespace $(NAMESPACE) --cpus=4 --memory=10240 --mount --mount-string "$(PWD)/:/project" --mount-uid $(UID) --mount-gid $(GID)

down: ## Stop the development environment
	@tilt down $(DEV_PROFILES) --context $(MINIKUBE_PROFILE)

dashboard: ## Start minikube dashboard web site that allows to see deployed resources
	@minikube dashboard --url -p $(MINIKUBE_PROFILE)

stop: ## Stop the development environment
	@minikube stop -p $(MINIKUBE_PROFILE)

clean_frontend: ## Remove storage and old docker image
	@sudo rm -rf $(PWD)/frontend/node_modules

clean: clean_data clean_minikube clean_frontend

clean_data:
	-@sudo rm -rf $(PWD)/_data

clean_minikube:
	-@minikube ssh -p $(MINIKUBE_PROFILE) -- docker system prune

delete: clean ## Remove the project traces from your computer, code stay untouched
	@minikube delete -p $(MINIKUBE_PROFILE)

delete_minikube: clean_minikube ## Remove the project traces from your computer, code stay untouched
	@minikube delete -p $(MINIKUBE_PROFILE)

init_python_env: ## Install the python common module to launch tests and make IDE working
	@echo "Creating virtual env"
	@python3.10 -m venv .venv
	@bash -c "source .venv/bin/activate && echo 'Virtual environment activated!' && env | grep -E '^[^#]*=' && echo 'Environment variables loaded!'"

# =====================================
# Tests
# =====================================

run_python_test:
	@PYTHONPATH="$PWD/deployments/config/schemas:$PYTHONPATH" python -m coverage run $(APPEND) --rcfile=base-service/.coveragerc --branch --context=$(git rev-parse --short HEAD) --source=./$(MODULE)/ -m pytest $(TYPE) --suppress-no-test-exit-code  --junitxml=report/$(MODULE)_report.xml -vv -c ./$(MODULE)/pytest.ini ./$(MODULE)/tests

component-test: ## Run the test for a given module MODULE=search
	@TYPE="-m component" make run_python_test
	@coverage report

unit-test: ## Run the test for a given module MODULE=search
	@TYPE="-m unit" make run_python_test
	@coverage report

focus_python_test: ## Runs only the test marked as only so we can focus on a single test
	@TYPE="-m only" make run_python_test


# =====================================
# HELP
# =====================================
help: ## Show this help
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

$(SKIP_TARGETS):
	@echo "Skipping $@ because .env does not exist."

$(SKIP_TARGETS_DB):
	@echo "Skipping $@ because .envdb does not exist."