BASEDIR  = $(shell pwd)
REBAR = $(shell pwd)/rebar3
.PHONY: rel test
APPNAME = gingko
RELPATH = _build/default/rel/distFlow
DEV1RELPATH = _build/dev1/rel/rcl_memkv
DEV2RELPATH = _build/dev2/rel/rcl_memkv
DEV3RELPATH = _build/dev3/rel/rcl_memkv

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
	rm -rf _build/

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




devrel1:
	$(REBAR) as dev1 release

devrel2:
	$(REBAR) as dev2 release

devrel3:
	$(REBAR) as dev3 release

devrel: devrel1 devrel2 devrel3


dev1-attach:
	$(BASEDIR)/_build/dev1/rel/gingko/bin/$(APPNAME) remote_console

dev2-attach:
	$(BASEDIR)/_build/dev2/rel/gingko/bin/$(APPNAME) remote_console

dev3-attach:
	$(BASEDIR)/_build/dev3/rel/gingko/bin/$(APPNAME) remote_console

dev1-console:
	$(BASEDIR)/_build/dev1/rel/gingko/bin/$(APPNAME) console

dev2-console:
	$(BASEDIR)/_build/dev2/rel/gingko/bin/$(APPNAME) console

dev3-console:
	$(BASEDIR)/_build/dev3/rel/gingko/bin/$(APPNAME) console


devrel-start:
	for d in $(BASEDIR)/_build/dev*; do $$d/rel/gingko/bin/$(APPNAME) daemon; done

devrel-stop:
	for d in $(BASEDIR)/_build/dev*; do $$d/rel/gingko/bin/$(APPNAME) stop; done

devrel-join:
	for d in $(BASEDIR)/_build/dev{2,3}; do $$d/rel/gingko/bin/$(APPNAME) eval 'riak_core:join("gingko1@127.0.0.1").'; done

devrel-cluster-plan:
	$(BASEDIR)/_build/dev1/rel/gingko/bin/$(APPNAME) eval 'riak_core_claimant:plan().'

devrel-cluster-commit:
	$(BASEDIR)/_build/dev1/rel/gingko/bin/$(APPNAME) eval 'riak_core_claimant:commit().'