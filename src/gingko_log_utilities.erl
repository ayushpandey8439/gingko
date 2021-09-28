%% @doc Utility functions for filtering log entries.
-module(gingko_log_utilities).

-include("gingko.hrl").

%% API
-export([
  filter_terms_for_key/8
]).


%% @doc Given a list of log_records, this method filters the ones corresponding to Key.
%% If key is undefined then is returns all records for all keys
%% It returns a dict corresponding to all the ops matching Key and
%% a list of the committed operations for that key which have a smaller commit time than MinSnapshotTime.
%%
%% @param OtherRecords list of log record tuples, {index, payload}
%% @param Key key to filter
%% @param MinSnapshotTime minimal snapshot time
%% @param MaxSnapshotTime maximal snapshot time
%% @param Ops dict accumulator for any type of operation records (possibly uncommitted)
%% @param CommittedOpsDict dict accumulator for committed operations
%% @returns a {dict, dict} tuple with accumulated operations and committed operations for key and snapshot filter
-spec filter_terms_for_key(
    txid(),
    [#log_read{}],
    key(),
    snapshot_time(),
    snapshot_time(),
    maps:map(txid(), [any_log_payload()]),
    maps:map(key(), [#clocksi_payload{}]),
    [#log_index{}]
) -> {
  maps:map(txid(), [any_log_payload()]),
  maps:map(key(), [#clocksi_payload{}]),
  [#log_index{}]
}.
filter_terms_for_key(TxId, [], _Key, _MinSnapshotTime, _MaxSnapshotTime, Ops, CommittedOps, Continuations) ->
  %add_ops_from_current_txn(TxId, Ops, CommittedOps),
  {Ops, CommittedOps, Continuations};

filter_terms_for_key(TxId, [#log_read{log_entry = {LSN, LogRecord}, continuation = Continuation} | OtherRecords], Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps,Continuations) ->
  #log_record{log_operation = LogOperation} = check_log_record_version(LogRecord),

  #log_operation{tx_id = LogTxId, op_type = OpType, log_payload = OpPayload} = LogOperation,
  case OpType of
    update ->
      handle_update(TxId, LogTxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps, Continuations);
    commit ->
      handle_commit(TxId, LogTxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps, Continuations, Continuation);
    _ ->
      filter_terms_for_key(TxId, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps, Continuations)
  end.


%% @doc Handles one 'update' log record
%%      Filters according to key and snapshot times
%%      If filter matches, appends payload to operations accumulator
-spec handle_update(
    txid(),
    txid(),                                   % used to identify tx id for the 'handle_commit' function
    #update_log_payload{},                    % update payload read from the log
    [#log_read{}],                            % rest of the log
    key(),                                    % filter for key
    snapshot_time() | ignore,                 % minimum snapshot time
    snapshot_time(),                          % maximum snapshot time
    maps:map(txid(), [any_log_payload()]),   % accumulator for any type of operation records (possibly uncommitted)
    maps:map(key(), [#clocksi_payload{}]),   % accumulator for committed operations
    [#log_index{}]                            % List of continuations used for indexing the log
) -> {
  maps:map(txid(), [any_log_payload()]),     % all accumulated operations for key and snapshot filter
  maps:map(key(), [#clocksi_payload{}]),     % accumulated committed operations for key and snapshot filter
  [#log_index{}]
}.
handle_update(TxId, LogTxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps, Continuations) ->
  #update_log_payload{key = PayloadKey} = OpPayload,
  case (Key == PayloadKey) of
    true ->
      % key matches: append to all operations accumulator
      TxnOps = maps:get(LogTxId, Ops, []),
      filter_terms_for_key(TxId, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, maps:put(LogTxId, lists:append(TxnOps,[OpPayload]), Ops), CommittedOps, Continuations);
    false ->
      % key does not match: skip
      filter_terms_for_key(TxId, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps, Continuations)
  end.


%% @doc Handles one 'commit' log record
%%      Filters according to key and snapshot times
%%      If filter matches, appends payload to operations accumulator
-spec handle_commit(
    txid(),
    txid(),                                   % searches for operations belonging to this tx id
    #commit_log_payload{},                    % update payload read from the log
    [#log_read{}],                            % rest of the log
    key(),                                    % filter for key
    snapshot_time() | ignore,                 % minimum snapshot time
    snapshot_time(),                          % maximum snapshot time
    maps:map(txid(), [any_log_payload()]),   % accumulator for any type of operation records (possibly uncommitted)
    maps:map(key(), [#clocksi_payload{}]),   % accumulator for committed operations
    [#log_index{}],                           % List of continuations for all the commits in the ops list
    continuation()                            % The point in the log at which this commit can be read
) -> {
  maps:map(txid(), [any_log_payload()]),     % all accumulated operations for key and snapshot filter
  maps:map(key(), [#clocksi_payload{}]),     % accumulated committed operations for key and snapshot filter
  [#log_index{}]
}.
handle_commit(TxId, LogTxId, OpPayload, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict, Continuations, Continuation) ->
  #commit_log_payload{commit_time = {DcId, TxCommitTime}, snapshot_time = SnapshotTime} = OpPayload,
  NewContinuations = Continuations ++ [#log_index{key = Key, snapshot_time = SnapshotTime, continuation = Continuation}],
  case maps:get(LogTxId, Ops, error) of
    error ->
      logger:debug("No Ops found for the transaction. LogTxnId is ~p and Operations are ~p",[LogTxId, Ops]),
      filter_terms_for_key(TxId, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict, Continuations);
    OpsList ->
      logger:debug("Ops found for the transaction, ~p",[OpsList]),
      NewCommittedOpsDict = getCommittedOps(TxId, DcId, LogTxId, TxCommitTime,OpsList, SnapshotTime, MinSnapshotTime, MaxSnapshotTime, CommittedOpsDict),
      filter_terms_for_key(TxId, OtherRecords, Key, MinSnapshotTime, MaxSnapshotTime, maps:remove(LogTxId, Ops), NewCommittedOpsDict, NewContinuations)
  end.


%% @doc Check the version of the log record and convert
%% to a different version if necessary
%% Checked when loading the log from disk, or
%% when log messages are received from another DC
-spec check_log_record_version(#log_record{}) -> #log_record{}.
check_log_record_version(LogRecord) ->
  %% Only support one version for now
  ?LOG_RECORD_VERSION = LogRecord#log_record.version,
  LogRecord.

getCommittedOps(_TxId, _DcId, _LogTxId, _TxCommitTime,[],_SnapshotTime, _MinSnapshotTime, _MaxSnapshotTime, CommittedOps) ->
  CommittedOps;
getCommittedOps(TxId, DcId, LogTxId, TxCommitTime, [#update_log_payload{key = KeyInternal, type = Type, op = Op}|OpsList],SnapshotTime, MinSnapshotTime, MaxSnapshotTime, CommittedOps)->
  NewCommittedOps = case (clock_comparision:check_min_time_gt(SnapshotTime, MinSnapshotTime) andalso
    clock_comparision:check_max_time_le(SnapshotTime, MaxSnapshotTime)) of
      true ->
        CommittedDownstreamOp =
          #clocksi_payload{
          key = KeyInternal,
          type = Type,
          op_param = Op,
          snapshot_time = SnapshotTime,
          commit_time = {DcId, TxCommitTime},
          txid = LogTxId},
        Ops = maps:get(KeyInternal, CommittedOps,[]),
        maps:put(KeyInternal, lists:append(Ops,[CommittedDownstreamOp]), CommittedOps);
      false ->
        CommittedOps
  end,
  getCommittedOps(TxId, DcId, LogTxId, TxCommitTime,OpsList, SnapshotTime, MinSnapshotTime, MaxSnapshotTime, NewCommittedOps).



add_ops_from_current_txn(TxId, Ops, CommittedOps) ->
  case maps:get(TxId, Ops, error) of
    error -> logger:debug("No uncommitted Operations");
    Operations ->  logger:debug("Operations are : ~p",[Operations])
  end.