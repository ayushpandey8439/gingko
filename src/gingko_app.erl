-module(gingko_app).

-behaviour(application).

-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case gingko_sup:start_link() of
      {ok, Pid} ->
          ok = riak_core:register([{vnode_module, gingko_vnode}]),
          ok = riak_core_node_watcher:service_up(gingko, self()), % This will be used when getting the primary APL in the Vnode masters.

          {ok, Pid};
      {error, Reason} ->
          {error, Reason}
    end.

stop(_State) ->
    ok.
