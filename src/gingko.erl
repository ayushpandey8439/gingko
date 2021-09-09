%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Antidote Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc The main interface for the persistent backend for CRDT objects called gingko.
%% The API provides functions to update, commit, abort, and read (get_version) keys.
%% Stable snapshots are currently not implemented, thus set_stable is a NO-OP.
-module(gingko).
-include("gingko.hrl").

%%---------------- API -------------------%%
-export([
  update/4,
  commit/4,
  commit/3,
  abort/2,
  get_version/2,
  get_version/4,
  set_stable/1, %%TODO: Implement for the checkpoint store,
  get_stats/0
]).
-export([start/1]).

%% @doc Start the logging server.
-spec start(integer()) -> {ok, pid()} | ignore | {error, term()}.
start(Partition) -> gingko_sup:start_link(Partition).


%%====================================================================
%% API functions
%%====================================================================

%% @equiv get_version(Key, Type, undefined)
-spec get_version(key(), type()) -> {ok, snapshot()}.
get_version(Key, Type) ->
  get_version(Key, Type, ignore, ignore).
%% New so the minimum timestamp is irrelevant and the last stale version in the cache is returned.


%% @doc Retrieves a materialized version of the object at given key with expected given type.
%% If MaximumSnapshotTime is given, then the version is guaranteed to not be older than the given snapshot.
%%
%% Example usage:
%%
%% Operations of a counter @my_counter in the log: +1, +1, -1, +1(not committed), -1(not committed).
%%
%% 2 = get_version(my_counter, antidote_crdt_counter_pn, undefined)
%%
%% @param Key the Key under which the object is stored
%% @param Type the expected CRDT type of the object
%% @param MaximumSnapshotTime if not 'undefined', then retrieves the latest object version which is not older than this timestamp
-spec get_version(key(), type(), snapshot_time(),snapshot_time()) -> {ok, snapshot()}.
get_version(Key, Type, MinimumSnapshotTime, MaximumSnapshotTime) ->
  io:format("."),
  logger:debug(#{function => "GET_VERSION", key => Key, type => Type, min_snapshot_timestamp => MinimumSnapshotTime, max_snapshot_timestamp => MaximumSnapshotTime}),

  %% Ask the cache for the object.
  %% If tha cache has that object, it is returned.
  %% If the cache does not have it, it is materialised from the log and stored in the cache.
  %% All subsequent reads of the object will return from the cache without reading the whole log.

  {ok, {Key, Type, Value, Timestamp}} = cache_daemon:get_from_cache(Key,Type,MinimumSnapshotTime,MaximumSnapshotTime),
  logger:debug(#{step => "materialize", materialized => {Key, Type, Value, Timestamp}}),
  {ok, {Key, Type, Value}}.


%% @doc Applies an update for the given key for given transaction id with a calculated valid downstream operation.
%% It is currently not checked if the downstream operation is valid for given type.
%% Invalid downstream operations will corrupt a key, which will cause get_version to throw an error upon invocation.
%%
%% A update log record consists of the transaction id, the op_type 'update' and the actual payload.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Key the Key under which the object is stored
%% @param Type the expected CRDT type of the object
%% @param TransactionId the id of the transaction this update belongs to
%% @param DownstreamOp the calculated downstream operation of a CRDT update
-spec update(key(), type(), txid(), op()) -> ok | {error, reason()}.
update(Key, Type, TransactionId, DownstreamOp) ->
  io:format("."),
  logger:debug(#{function => "UPDATE", key => Key, type => Type, transaction => TransactionId, op => DownstreamOp}),

  Entry = #log_operation{
      tx_id = TransactionId,
      op_type = update,
      log_payload = #update_log_payload{key = Key, type = Type , op = DownstreamOp}},

  LogRecord = #log_record {
    version = ?LOG_RECORD_VERSION,
    op_number = #op_number{},        % not used
    bucket_op_number = #op_number{}, % not used
    log_operation = Entry
  },
  gingko_op_log:append(?LOGGING_MASTER, Key, LogRecord).


%% @doc Commits all operations belonging to given transaction id for given list of keys.
%%
%% A commit log record consists of the transaction id, the op_type 'commit'
%% and the actual payload which consists of the commit time and the snapshot time.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to commit
%% @param TransactionId the id of the transaction this commit belongs to
%% @param CommitTime TODO
%% @param SnapshotTime TODO
-spec commit([key()], txid(), dc_and_commit_time()) -> ok.
commit(Keys, TransactionId, CommitTime)->
  commit(Keys, TransactionId, CommitTime, vectorclock:new()).

-spec commit([key()], txid(), dc_and_commit_time(), snapshot_time()) -> ok.
commit(Keys, TransactionId, CommitTime, SnapshotTime) ->
  io:format("."),
  logger:debug(#{function => "COMMIT", keys => Keys, transaction => TransactionId, commit_timestamp => CommitTime, snapshot_timestamp => SnapshotTime}),

  Entry = #log_operation{
      tx_id = TransactionId,
      op_type = commit,
      log_payload = #commit_log_payload{commit_time = CommitTime, snapshot_time = SnapshotTime}},

  LogRecord = #log_record {
    version = ?LOG_RECORD_VERSION,
    op_number = #op_number{},        % not used
    bucket_op_number = #op_number{}, % not used
    log_operation = Entry
  },

  lists:map(fun(Key) -> gingko_op_log:append(?LOGGING_MASTER, Key, LogRecord) end, Keys),
  ok.

%% @doc Aborts all operations belonging to given transaction id for given list of keys.
%%
%% An abort log record consists of the transaction id, the op_type 'abort'
%% and the actual payload which is empty.
%% It is wrapped again in a record for future use in the possible distributed gingko backend
%% and for compatibility with the current Antidote backend.
%%
%% @param Keys list of keys to abort a transaction
%% @param TransactionId the id of the transaction to abort
-spec abort([key()], txid()) -> ok.
abort(Keys, TransactionId) ->
  logger:debug(#{function => "ABORT", keys => Keys, transaction => TransactionId}),

  Entry = #log_operation{
      tx_id = TransactionId,
      op_type = abort,
      log_payload = #abort_log_payload{}},

  LogRecord = #log_record {
    version = ?LOG_RECORD_VERSION,
    op_number = #op_number{},        % not used
    bucket_op_number = #op_number{}, % not used
    log_operation = Entry
  },

  lists:map(fun(Key) -> gingko_op_log:append(?LOGGING_MASTER, Key, LogRecord) end, Keys),
  ok.


%% @doc Sets a timestamp for when all operations below that timestamp are considered stable.
%%
%% Currently not implemented.
%% @param SnapshotTime TODO
-spec set_stable(snapshot_time()) -> ok.
set_stable(SnapshotTime) ->
  logger:warning(#{function => "SET_STABLE", timestamp => SnapshotTime, message => "not implemented"}),
  ok.


get_stats() ->
  gen_server:call(?CACHE_DAEMON, {get_event_stats}).
