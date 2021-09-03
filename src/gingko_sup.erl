%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko_sup).

-behaviour(supervisor).
-include("gingko.hrl").
%% API
-export([start_link/0]).
%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
  Gingko_VNode_master =
        {gingko_vnode_master,
         {riak_core_vnode_master, start_link, [gingko_vnode]},
         permanent,
         5000,
         worker,
         [gingko_vnode_master]},


  Worker = {?LOGGING_MASTER,
    {?LOGGING_MASTER, start_link, ["main_log", none]},
    permanent, 5000, worker, [?LOGGING_MASTER]},

  CacheDaemon = {?CACHE_DAEMON,
    {?CACHE_DAEMON,start_link,[gingko_cache, 2 , 200000]},
    permanent,5000,worker,[?CACHE_DAEMON]},

  CheckpointDaemon = {checkpoint_daemon,
    {checkpoint_daemon,start_link,[gingko_checkpoint_store]},
    permanent,5000,worker,[checkpoint_daemon_server]},

  LogIndexDaemon = {?LOG_INDEX_DAEMON,
    {?LOG_INDEX_DAEMON,start_link,[gingko_log_index]},
    permanent,5000,worker,[?LOG_INDEX_DAEMON]},

    {ok, {{one_for_one, 5, 10}, [Gingko_VNode_master, Worker, CacheDaemon, CheckpointDaemon, LogIndexDaemon]}}.
