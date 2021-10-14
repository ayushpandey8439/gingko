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
-export([build/6, build/7]).

%% TODO: Make this an independent actor to facilitate concurrency.
-spec build(term(), atom(),atom(), vectorclock:vectorclock() | ignore, vectorclock:vectorclock(), integer()) -> {integer(),snapshot()} | {integer(),{error, {unexpected_operation, effect(), type()}}}.
-spec build(term(), atom(),atom(),snapshot(), vectorclock:vectorclock() | ignore, vectorclock:vectorclock(), integer()) -> {integer(),snapshot()} | {integer(),{error, {unexpected_operation, effect(), type()}}}.
build(TxId, Key, Type, MinSnapshotTime, MaximumSnapshotTime, Partition) ->
  build(TxId, Key, Type, gingko_materializer:create_snapshot(Type), MinSnapshotTime, MaximumSnapshotTime, Partition).
build(TxId, Key, Type, BaseSnapshot, MinSnapshotTime, MaximumSnapshotTime, Partition) ->
  % Go to the index and get the minimum continuation we can start from.
  {ok, ContinuationObject} = log_index_daemon:get_continuation(Key, MinSnapshotTime, Partition),
  % TODO: In the cached version when the cache is invalidated, we need to check if the continuiation we have needs to be deleted also
  % TODO: Or if there is another way we can check that the cached version can be rebiult without
  % With the list of log entries for the key, we also have the list of continuation objects.
  {ok, Data} = gingko_op_log:read_log_entries(ContinuationObject, Partition),
  logger:error(#{step  => "unfiltered log", payload => Data, snapshot_timestamp => MaximumSnapshotTime}),
  {Ops, CommittedOps, FilteredContinuations} = gingko_log_utilities:filter_terms_for_key(Data, Key, MinSnapshotTime, MaximumSnapshotTime, maps:new(), maps:new(),[]),
  logger:debug(#{step => "filtered terms",transaction => TxId , ops => Ops, committed => CommittedOps}),

  CurrentUnappliedOps = case maps:find(TxId, Ops) of
    {ok, List} -> List;
    error -> []
  end,
  PayloadForKey = case maps:get(Key, CommittedOps, error) of
    error -> [];
    Entry -> Entry
  end,
  % The payload for the key can also contain entries that are not possibly committed. These can be added back to the payload entry and can be used to read items within an interactive txn
  %% Get the clock of the last operation committed for the key and use it as the cache timestamp.
  {SnapshotTimestamp, Materialization} = case PayloadForKey == [] of
    true ->
          {MinSnapshotTime, BaseSnapshot};
    false ->
      % Index the object materialization with the continuation
      %TODO Possible optimization could be to only index one item instead of all the operations.
      lists:foreach(fun(#log_index{key = Key, snapshot_time = SnapshotTime, continuation = Continuation}) -> log_index_daemon:add_to_index(Key, SnapshotTime,Continuation, Partition) end, FilteredContinuations),
      LastCommittedOperation = lists:last(PayloadForKey),
      LastCommittedOpSnapshotTime = LastCommittedOperation#clocksi_payload.snapshot_time,
      ClockSIMaterialization = gingko_materializer:materialize_clocksi_payload(Type, BaseSnapshot, PayloadForKey),
      {LastCommittedOpSnapshotTime, ClockSIMaterialization}
  end,
  InteractiveMaterialization = gingko_materializer:materialize_uncommitted(Type, Materialization, CurrentUnappliedOps),
  {SnapshotTimestamp, Materialization, InteractiveMaterialization}.