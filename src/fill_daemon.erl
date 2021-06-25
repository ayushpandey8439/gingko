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
-export([build/4, build/5]).

%% TODO: Make this an independent actor to facilitate concurrency.
-spec build(atom(),atom(), vectorclock:vectorclock() | ignore, vectorclock:vectorclock()) -> {integer(),snapshot()} | {integer(),{error, {unexpected_operation, effect(), type()}}}.
-spec build(atom(),atom(),snapshot(), vectorclock:vectorclock() | ignore, vectorclock:vectorclock()) -> {integer(),snapshot()} | {integer(),{error, {unexpected_operation, effect(), type()}}}.
build(Key, Type, MinSnapshotTime, MaximumSnapshotTime) ->
  build(Key, Type, materializer:create_snapshot(Type), MinSnapshotTime, MaximumSnapshotTime).
build(Key, Type, BaseSnapshot, MinSnapshotTime, MaximumSnapshotTime) ->
  % Go to the index and get the minimum continuation we can start from.
  {ok, ContinuationObject} = log_index_daemon:get_continuation(Key,MinSnapshotTime),
  logger:info("Continuation for this version starts from ~p ~n",[ContinuationObject]),

  % With the list of log entries for the key, we also have the list of continuation objects.
  {ok, {Data, Continuations}} = gingko_op_log:read_log_entries(?LOGGING_MASTER, 0, all, ContinuationObject),
  logger:debug(#{step => "unfiltered log", payload => Data, snapshot_timestamp => MaximumSnapshotTime}),
  {Ops, CommittedOps} = log_utilities:filter_terms_for_key(Data, {key, Key}, MinSnapshotTime, MaximumSnapshotTime, dict:new(), dict:new()),
  logger:debug(#{step => "filtered terms", ops => Ops, committed => CommittedOps}),
  PayloadForKey = case dict:find(Key, CommittedOps) of
    {ok, Entry} -> Entry;
    error -> []
  end,
  logger:info(#{ops => Ops, committed => CommittedOps}),
  logger:info("Payload for key is: ~p",[PayloadForKey]),
  io:format("CommittedOps for Key are :: ~p ~n",[CommittedOps]),
  %% Get the clock of the last operation committed for the key and use it as the cache timestamp.
  {SnapshotTimestamp, Materialization} = case PayloadForKey == [] of
    true ->
      {vectorclock:new(), materializer:create_snapshot(Type)};
    false ->
      LastCommittedOperation = lists:last(PayloadForKey),
      LastCommittedOpSnapshotTime = LastCommittedOperation#clocksi_payload.snapshot_time,
      ClockSIMaterialization = materializer:materialize_clocksi_payload(Type, BaseSnapshot, PayloadForKey),
      {LastCommittedOpSnapshotTime, ClockSIMaterialization}
  end
% Index the object materialization with the continuation
.