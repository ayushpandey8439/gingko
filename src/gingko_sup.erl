-module(gingko_sup).
-behaviour(supervisor).
-include("gingko.hrl").
-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link(gingko_sup, [testpartition]).

init([Partition]) ->
    Worker = {?LOGGING_MASTER,
    {?LOGGING_MASTER, start_link, [integer_to_list(Partition)++"op_journal", none]},
    permanent, 5000, worker, [?LOGGING_MASTER]},

  CacheDaemon = {?CACHE_DAEMON,
    {?CACHE_DAEMON,start_link,[list_to_atom(integer_to_list(Partition)++"gingko_cache"), 2 , 200000]},
    permanent,5000,worker,[?CACHE_DAEMON]},

  CheckpointDaemon = {checkpoint_daemon,
    {checkpoint_daemon,start_link,[list_to_atom(integer_to_list(Partition)++"gingko_checkpoint_store")]},
    permanent,5000,worker,[checkpoint_daemon_server]},

  LogIndexDaemon = {?LOG_INDEX_DAEMON,
    {?LOG_INDEX_DAEMON,start_link,[list_to_atom(integer_to_list(Partition)++"gingko_log_index")]},
    permanent,5000,worker,[?LOG_INDEX_DAEMON]},

  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  {ok, {SupFlags, [Worker,CacheDaemon,CheckpointDaemon,LogIndexDaemon]}}.
