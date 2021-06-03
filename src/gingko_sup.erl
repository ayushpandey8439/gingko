-module(gingko_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link(gingko_sup, []).

init(_Args) ->
  Worker = {gingko_op_log_server,
    {gingko_op_log_server, start_link, ["main_log", none]},
    permanent, 5000, worker, [gingko_op_log_server]},

  CacheManager = {cache_daemon_server,
    {cache_daemon_server,start_link,[gingko_cache]},
    permanent,5000,worker,[cache_daemon_server]},

  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  {ok, {SupFlags, [Worker,CacheManager]}}.
