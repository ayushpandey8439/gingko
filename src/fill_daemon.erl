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
-module(fill_daemon).
-include("gingko.hrl").

-author("pandey").

%% API
-export([build/3]).

%% TODO: Make this an independent actor to facilitate concurrency.
-spec build(atom(),atom(), vectorclock:vectorclock()) -> {integer(),snapshot()} | {integer(),{error, {unexpected_operation, effect(), type()}}}.
build(Key, Type, MaximumSnapshotTime) ->
  {ok, Data} = gingko_op_log:read_log_entries(?LOGGING_MASTER, 0, all),
  logger:debug(#{step => "unfiltered log", payload => Data, snapshot_timestamp => MaximumSnapshotTime}),

  {Ops, CommittedOps, LastLSN} = log_utilities:filter_terms_for_key(Data, {key, Key}, undefined, MaximumSnapshotTime, dict:new(), dict:new(),0),
  %% Tha added 0 is to get the last LSN for a particular key which was materialised. This will be used when the object becomes stale.
  logger:debug(#{step => "filtered terms", ops => Ops, committed => CommittedOps}),
  PayloadForKey = case dict:find(Key, CommittedOps) of
    {ok, Entry} -> Entry;
    error -> []
  end,
  logger:info(#{ops => Ops, committed => CommittedOps}),
  ClockSIMaterialization = materializer:materialize_clocksi_payload(Type, materializer:create_snapshot(Type), PayloadForKey),
  {LastLSN, ClockSIMaterialization}.
