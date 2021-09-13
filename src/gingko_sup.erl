-module(gingko_sup).
-behaviour(supervisor).
-include("gingko.hrl").
-export([start_link/1]).
-export([init/1]).

start_link(Partition) ->
  supervisor:start_link(gingko_sup, [Partition]).

init([Partition]) ->
    Worker = {?LOGGING_MASTER,
    {?LOGGING_MASTER, start_link, ["main_log", none, Partition]},
    permanent, 5000, worker, [?LOGGING_MASTER]},

  CacheDaemon = {?CACHE_DAEMON,
    {?CACHE_DAEMON,start_link,[gingko_cache, 2 , 200000, Partition]},
    permanent,5000,worker,[?CACHE_DAEMON]},

  LogIndexDaemon = {?LOG_INDEX_DAEMON,
    {?LOG_INDEX_DAEMON,start_link,[gingko_log_index, Partition]},
    permanent,5000,worker,[?LOG_INDEX_DAEMON]},

  SupFlags = #{strategy => one_for_all, intensity => 1, period => 5},
  {ok, {SupFlags, [Worker, CacheDaemon,LogIndexDaemon]}}.
