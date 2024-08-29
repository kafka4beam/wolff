all: eunit cover

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

.PHONY: eunit
eunit:
	@rebar3 eunit -v

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
