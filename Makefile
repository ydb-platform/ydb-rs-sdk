CODEGEN_IMAGE ?= ydb-rs-sdk-grpc-codegen
CODEGEN_DOCKERFILE ?= ydb-grpc/generate-protobuf.Dockerfile
DOCKER_USER ?= $(shell id -u):$(shell id -g)

.PHONY: proto proto-image

proto: proto-image
	docker run --rm \
		--user "$(DOCKER_USER)" \
		-v "$(CURDIR):/workspace" \
		-w /workspace \
		-e CARGO_HOME=/tmp/cargo-home \
		-e CARGO_TARGET_DIR=/tmp/ydb-rs-sdk-grpc-codegen-target \
		$(CODEGEN_IMAGE) \
		sh -c 'cargo build --locked -p ydb-grpc --features regenerate-sources && cargo fmt -p ydb-grpc'

proto-image:
	docker build \
		-f $(CODEGEN_DOCKERFILE) \
		-t $(CODEGEN_IMAGE) \
		.
