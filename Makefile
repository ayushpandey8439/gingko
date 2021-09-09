BASEDIR  = $(shell pwd)
REBAR = $(shell pwd)/rebar3
.PHONY: rel test
RELPATH = _build/default/rel/distFlow


all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

shell:
	$(REBAR) release
	_build/default/rel/gingko/bin/gingko console

rel:
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

lint:
	${REBAR} as lint lint

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true --cover
	${REBAR} ct skip_deps=true --cover

	${REBAR} cover --verbose

dialyzer:
	${REBAR} dialyzer

edoc:
	${REBAR} edoc
	rm doc/erlang.png
	cp doc/ext/gingko.png doc/erlang.png

prerelease_check:test dialyzer
