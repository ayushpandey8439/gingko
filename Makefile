REBAR = $(shell pwd)/rebar3
.PHONY: rel test

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

shell:
	$(REBAR) shell --name='gingko@127.0.0.1'

rel:
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

lint:
	${REBAR} as lint lint

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

dialyzer:
	${REBAR} dialyzer

edoc:
	${REBAR} edoc
	rm doc/erlang.png
	cp doc/ext/gingko.png doc/erlang.png