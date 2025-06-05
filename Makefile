KAFKA_IMAGE_VERSION ?= 1.1.3
export KAFKA_IMAGE_VERSION
KAFKA_VERSION ?= 4.0.0
export KAFKA_VERSION

all: eunit ct cover

.PHONY: compile
compile: deps
	@rebar3 compile

.PHONY: deps
deps:
	@rebar3 get-deps

.PHONY: edoc
edoc:
	@rebar3 edoc

.PHONY: dialyzer
dialyzer: compile
	@rebar3 dialyzer

.PHONY: ct
ct:
	@rebar3 ct -v -c

.PHONY: eunit
eunit:
	@rebar3 eunit -v -c

.PHONY: clean
clean:
	@rebar3 clean
	@rm -rf test-data
	@rm -rf _build
	@rm -f rebar.lock

.PHONY: xref
xref: compile
	@rebar3 xref

.PHONY: hex
hex: clean
	@rebar3 hex publish package

.PHONY: cover
cover:
	@rebar3 cover -v

.PHONY: coveralls
coveralls:
	@rebar3 coveralls send

hex-publish: clean
	@rebar3 hex publish --repo=hexpm

.PHONY: test-env
test-env:
	@./scripts/setup-test-env.sh
