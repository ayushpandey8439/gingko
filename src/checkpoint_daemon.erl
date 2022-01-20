%%%-------------------------------------------------------------------
%%% @author ayushp
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(checkpoint_daemon).
-include("gingko.hrl").
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).
-export([get_checkpoint/3, updateKeyInCheckpoint/2, commitTxn/2,trigger_checkpoint/1]).
-define(SERVER, ?MODULE).

-record(checkpoint_daemon_state, {checkpoint_table:: atom(),
  checkpoints:: #{},
  txn_safe :: disk_log:continuation(),
  truncation_safe::disk_log:continuation(),
  txnset :: #{},
  partition :: partition_id()}).

-record(closestMatch, {clock :: vectorclock:vectorclock(), snapshot:: term()}).

%%%===================================================================
%%% External API
%%%===================================================================

get_checkpoint(Key, SnapshotTime, Partition) ->
  gen_server:call(list_to_atom(atom_to_list(?CHECKPOINT_DAEMON)++integer_to_list(Partition)), {get_checkpoint, Key, SnapshotTime}, infinity).

trigger_checkpoint(Partition) ->
    gen_server:cast(list_to_atom(atom_to_list(?CHECKPOINT_DAEMON)++integer_to_list(Partition)), {create_checkpoint}).

updateKeyInCheckpoint(Partition, TxnId) ->
  gen_server:call(list_to_atom(atom_to_list(?CHECKPOINT_DAEMON)++integer_to_list(Partition)), {updateKey, TxnId}).

commitTxn(Partition, TxnId) ->
    gen_server:call(list_to_atom(atom_to_list(?CHECKPOINT_DAEMON)++integer_to_list(Partition)), {commitTxn, TxnId}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CheckpointIdentifier, Partition) ->
  gen_server:start_link({local, list_to_atom(atom_to_list(?CHECKPOINT_DAEMON)++integer_to_list(Partition))}, ?MODULE, {CheckpointIdentifier, Partition}, []).


init({CheckpointIdentifier, Partition}) ->
  logger:notice(#{
    action => "Starting Checkpoint Daemon",
    registered_as => ?MODULE,
    pid => self()
  }),
  CheckpointTableName = list_to_atom(integer_to_list(Partition)++atom_to_list(CheckpointIdentifier)),
  CheckpointTable = case open_checkpoint_store(CheckpointTableName) of
                      {ok, Name} -> Name;
                      {error, Reason} -> terminate(Reason, #checkpoint_daemon_state{})
                    end,
  {ok, #checkpoint_daemon_state{checkpoint_table = CheckpointTable,
    checkpoints = #{},
    truncation_safe = start,
    txn_safe = start,
    txnset = maps:new(),
    partition = Partition}}.


handle_call({updateKey, TxnId}, _From, State = #checkpoint_daemon_state{txnset = TransactionSet,
  txn_safe = TransactionSafePointer}) ->
  UpdatedTransactionset = case maps:get(TxnId, TransactionSet, na) of
    na ->
      maps:put(TxnId, {TransactionSafePointer, ignore}, TransactionSet);
    _ ->
      TransactionSet
  end,
  {reply, ok, State#checkpoint_daemon_state{txnset = UpdatedTransactionset}};

handle_call({commitTxn, TxnId}, _From, State = #checkpoint_daemon_state{truncation_safe = TruncationSet}) ->
  % TODO: Get the last continuation from gingko_op_log
  UpdatedTruncationSet =
    case maps:get(TxnId, TruncationSet, na) of
      na ->
        maps:put(TxnId, {TransactionSafePointer, ignore}, TransactionSet);
        TransactionSet;
      _ ->
        TruncationSet
    end,
  {reply, ok, State#checkpoint_daemon_state{txnset = UpdatedTransactionset}};


handle_call({get_checkpoint, Key, SnapshotTime}, _From, State = #checkpoint_daemon_state{checkpoint_table = CheckpointTable}) ->
  Response = case checkpointLookup(CheckpointTable, Key, SnapshotTime) of
    {exact_match, SnapshotTime, Snapshot} ->
      {ok, SnapshotTime, Snapshot};
    {non_exact_match, MaxSnapshotTime, Snapshot} ->
      {ok, MaxSnapshotTime, Snapshot};
    {error, _Reason} ->
      {error, not_exist}
  end,
  {reply, Response, State}.

handle_cast({create_checkpoint}, State = #checkpoint_daemon_state{checkpoint_table = CheckpointTable, partition = Partition}) ->

  {ok, Data} = gingko_op_log:read_log_entries(start, Partition),
  {_Ops, CommittedOps, FilteredContinuations} = map_records_by_key(Data, ignore, ignore, maps:new()),
  case maps:get(Key, CommittedOps, error) of
    error -> [];
    PayloadForKey ->
      LastCommittedOperation = lists:last(PayloadForKey),
      LastCommittedOpSnapshotTime = LastCommittedOperation#clocksi_payload.snapshot_time,
      BaseSnapshot = case checkpointLookup(CheckpointTable, Key, LastCheckpointTime) of
                       {error, not_exist} ->
                         Type:new();
                       {_, _MatchClock, MatchSnapshot} ->
                         MatchSnapshot
                     end,
      ClockSIMaterialization = gingko_materializer:materialize_clocksi_payload(Type, BaseSnapshot, PayloadForKey),
      checkpointSnapshot(CheckpointTable, Key, LastCommittedOpSnapshotTime, ClockSIMaterialization),
      lists:foreach(fun(#log_index{key = LogKey, snapshot_time = SnapshotTime, continuation = Continuation}) -> log_index_daemon:add_to_index(LogKey, SnapshotTime,Continuation, Partition) end, FilteredContinuations)
  end,
  {noreply, State}.

handle_info(_Info, State = #checkpoint_daemon_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #checkpoint_daemon_state{checkpoint_table = CheckpointTable}) ->
  dets:close(CheckpointTable),
  ok.

code_change(_OldVsn, State = #checkpoint_daemon_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================



%% @doc Returns the base log dir path
-spec checkpoint_dir_base(node() | string()) -> string().
checkpoint_dir_base(LogName) when is_atom(LogName)->
  checkpoint_dir_base(atom_to_list(LogName));
checkpoint_dir_base(LogName) ->
  % read path
  EnvLogDir = os:getenv("checkpoint_root"),
  case EnvLogDir of
    false -> LogDir = "data/checkpoint/"; % default value if not set
    LogDir -> LogDir
  end,
  LogDir ++ LogName ++ "/".



%% @doc ensures directory where the log is expected and opens the log file.
%%      Recovers if required, logging found terms and bad bytes
open_checkpoint_store(CheckpointName) ->
  logger:debug("Trying to open checkpoint store ~p",[CheckpointName]),
  filelib:ensure_dir(checkpoint_dir_base(CheckpointName)),

  CheckpointFile = checkpoint_dir_base(CheckpointName) ++ "CHECKPOINTS",

  CheckpointOptions = [{auto_save, 10000}, {file, CheckpointFile}, {type, bag}],
  case dets:open_file(CheckpointFile, CheckpointOptions) of
    {ok, Name} -> {ok, Name};
    {error,Reason} ->
      logger:warning("Could not open checkpoint store. Error: ~p", [Reason]),
      {error, Reason}
  end.

checkpointLookup(CheckpointStore, Key, Clock) ->
  case dets:lookup(CheckpointStore, {Key, Clock}) of
    {error, Reason} ->
      {error, Reason};
    [] ->
      {error, not_exist};
    Snapshots when is_list(Snapshots) ->
      searchClosestSnapshot(Snapshots, Clock, #closestMatch{clock = vectorclock:new()});
    _ ->
      {error, improper_or_conflicting_entry}
  end.

checkpointSnapshot(CheckpointStore, Key, Clock, Snapshot) ->
  dets:insert(CheckpointStore, {Key, Clock, Snapshot}).

searchClosestSnapshot([{_Key, Clock, Snapshot} | _Rest], Clock, _ClosestMatch) ->
  {exact_match, Clock, Snapshot};
searchClosestSnapshot([], _Clock, _MaxSnapshotFound = #closestMatch{clock = MatchClock, snapshot = MatchSnapshot}) ->
  {non_exact_match, MatchClock, MatchSnapshot};
searchClosestSnapshot([{_Key, Clock1, Snapshot} | Rest], Clock, ClosestMatch = #closestMatch{clock = MatchClock}) ->
  CheckpointClockGreaterThanCurrentMatch = vectorclock:all_dots_greater(Clock1, MatchClock),
  CheckpointClockSmallerThanExpected = vectorclock:all_dots_smaller(Clock1, Clock),
  if
    CheckpointClockGreaterThanCurrentMatch == true andalso  CheckpointClockSmallerThanExpected == true ->
      searchClosestSnapshot(Rest, Clock, #closestMatch{clock = Clock1, snapshot = Snapshot});
    true ->
      searchClosestSnapshot(Rest, Clock, ClosestMatch)
  end.

map_records_by_key([], _MinSnapshotTime, _MaxSnapshotTime, Ops, CommittedOps) ->
  %add_ops_from_current_txn(TxId, Ops, CommittedOps),
  {Ops, CommittedOps};

map_records_by_key([#log_read{log_entry = {_LSN, LogRecord}, continuation = Continuation} | OtherRecords],MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps) ->
 LogOperation = LogRecord#log_record.log_operation,
 #log_operation{tx_id = LogTxId, op_type = OpType, log_payload = OpPayload} = LogOperation,
  case OpType of
    update ->
      handle_update(LogTxId, OpPayload, OtherRecords, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps);
    commit ->
      handle_commit(LogTxId, OpPayload, OtherRecords, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps);
    _ ->
      map_records_by_key(OtherRecords, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps)
  end.



handle_update(LogTxId, OpPayload, OtherRecords, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOps) ->
  TxnOps = maps:get(LogTxId, Ops, []),
  map_records_by_key(OtherRecords, MinSnapshotTime, MaxSnapshotTime, maps:put(LogTxId, lists:append(TxnOps,[OpPayload]), Ops), CommittedOps).

handle_commit(LogTxId, OpPayload, OtherRecords, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
  #commit_log_payload{commit_time = {DcId, TxCommitTime}, snapshot_time = SnapshotTime} = OpPayload,
  case maps:get(LogTxId, Ops, error) of
    error ->
      logger:debug("No Ops found for the transaction. LogTxnId is ~p and Operations are ~p",[LogTxId, Ops]),
      map_records_by_key(OtherRecords, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict);
    OpsList ->
      logger:debug("Ops found for the transaction, ~p",[OpsList]),
      NewCommittedOpsDict = getCommittedOps(DcId, LogTxId, TxCommitTime,OpsList, SnapshotTime, MinSnapshotTime, MaxSnapshotTime, CommittedOpsDict),
      map_records_by_key(OtherRecords, MinSnapshotTime, MaxSnapshotTime, maps:remove(LogTxId, Ops), NewCommittedOpsDict)
  end.


getCommittedOps(_DcId, _LogTxId, _TxCommitTime,[],_SnapshotTime, _MinSnapshotTime, _MaxSnapshotTime, CommittedOps) ->
  CommittedOps;
getCommittedOps(DcId, LogTxId, TxCommitTime, [#update_log_payload{key = KeyInternal, type = Type, op = Op}|OpsList],SnapshotTime, MinSnapshotTime, MaxSnapshotTime, CommittedOps)->
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
  getCommittedOps(DcId, LogTxId, TxCommitTime,OpsList, SnapshotTime, MinSnapshotTime, MaxSnapshotTime, NewCommittedOps).
