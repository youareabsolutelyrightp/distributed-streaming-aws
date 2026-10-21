SHELL := /bin/bash
.DEFAULT_GOAL := check

include versions.env
export

ROOT := $(CURDIR)
M2 := $(ROOT)/.cache/m2
GO_CACHE := $(ROOT)/.cache/go
MAVEN := docker run --rm --env-file $(ROOT)/versions.env \
	--env HOME=/tmp/maven-home --user $(shell id -u):$(shell id -g) \
	-v $(ROOT):/workspace -v $(M2):/workspace/.cache/m2 \
	-w /workspace/processor/flink $(MAVEN_IMAGE) \
	mvn -Dmaven.repo.local=/workspace/.cache/m2
GO := docker run --rm --user $(shell id -u):$(shell id -g) \
	-e GOCACHE=/workspace/.cache/go/build -e GOMODCACHE=/workspace/.cache/go/mod \
	-v $(ROOT):/workspace $(GO_IMAGE)

.PHONY: build test check benchmark clean

build:
	@mkdir -p $(M2) $(GO_CACHE)
	$(MAVEN) -q -DskipTests package
	$(GO) sh -c 'cd /workspace/producer && go build ./cmd/taxi-replay'
	$(GO) sh -c 'cd /workspace/materializer && go build ./cmd/taxi-materializer'
	$(GO) sh -c 'cd /workspace/data/tools && go build ./cmd/taxi-data'

test:
	@mkdir -p $(M2) $(GO_CACHE)
	$(MAVEN) -q test
	$(GO) sh -c 'cd /workspace/producer && go test ./...'
	$(GO) sh -c 'cd /workspace/materializer && go test ./...'
	$(GO) sh -c 'cd /workspace/data/tools && go test ./...'
	docker build -q -t taxi-reference:test processor/reference >/dev/null
	docker run --rm --user $(shell id -u):$(shell id -g) \
		-v $(ROOT)/processor/reference:/workspace -w /workspace \
		-v $(ROOT)/schemas:/contracts:ro \
		taxi-reference:test python3 -m unittest discover -s tests
	PYTHONPATH=$(ROOT) python3 -m unittest discover -s tests
	docker run --rm -v $(ROOT):/workspace -w /workspace \
		$(AWS_CONTROL_PYTHON_IMAGE) python -m unittest \
		tests.test_benchmark.RepositoryTest.test_runtime_contract_schemas_resolve_from_release_root \
		tests.test_benchmark.RepositoryTest.test_transport_v3_analyzer_cli_reconstructs_the_export_shape

check: test
	@bash -n infra/runtime/*.sh
	@python3 -m py_compile aws_benchmark/*.py aws_benchmark/runtime_tools/*.py
	@PYTHONPATH=$(ROOT) ./benchmark check
	@docker run --rm -v $(ROOT):/workspace -w /workspace/infra/terraform \
		$(TERRAFORM_IMAGE) fmt -check -recursive
	@docker run --rm -v $(ROOT):/workspace -w /workspace/infra/terraform \
		$(TERRAFORM_IMAGE) init -backend=false -input=false >/dev/null
	@docker run --rm -v $(ROOT):/workspace -w /workspace/infra/terraform \
		$(TERRAFORM_IMAGE) validate
	@! find . -type f \( -name '*.tfstate' -o -name '*.pem' -o -name '*.key' \) \
		-not -path './.runs/*' -print | grep .
	@! find . -type f -size +50M \
		-not -path './.git/*' -not -path './.cache/*' -not -path './.runs/*' \
		-not -path './infra/terraform/.terraform/*' -not -path './processor/flink/target/*' \
		-print | grep .

benchmark:
	@./benchmark run

clean:
	@find . -type d \( -name target -o -name __pycache__ \) -prune -exec rm -rf {} +
	@rm -rf .cache infra/terraform/.terraform
	@rm -f producer/taxi-replay materializer/taxi-materializer data/tools/taxi-data
