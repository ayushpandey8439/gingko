%%%-------------------------------------------------------------------
%%% @author kevin
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Okt 2019 17:45
%%%-------------------------------------------------------------------
-module(gingko_utils).
-author("kevin").
-include("gingko.hrl").

%% API
-export([create_new_snapshot/1, is_in_vts_range/2, get_clock_range/2, is_in_clock_range/2, create_cache_entry/1, create_default_value/1, get_jsn_number/1, sort_by_jsn_number/1, is_system_operation/2, is_update_of_keys/2, is_update/1, get_keys_from_updates/1, generate_downstream_op/4, create_snapshot_from_cache_entry/1, get_timestamp/0, contains_system_operation/2, is_update_of_keys_or_commit/2, get_dcid/0, update_cache_usage/2, get_latest_vts/1, get_DCSf_vts/0, get_GCSf_vts/0]).

-spec get_timestamp() -> non_neg_integer().
get_timestamp() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega * 1000000 + Sec) * 1000000 + Micro. %TODO check if this is a good solution

-spec get_dcid() -> dcid().
get_dcid() -> undefined. %TODO implement

-spec get_DCSf_vts() -> vectorclock().
get_DCSf_vts() -> vectorclock:new(). %TODO implement

-spec get_GCSf_vts() -> vectorclock().
get_GCSf_vts() -> vectorclock:new(). %TODO implement

-spec is_in_vts_range(vectorclock(), vts_range()) -> boolean().
is_in_vts_range(Vts, VtsRange) ->
  case VtsRange of
    {none, none} -> true;
    {none, MaxVts} -> vectorclock:le(Vts, MaxVts);
    {MinVts, none} -> vectorclock:le(MinVts, Vts);
    {MinVts, MaxVts} -> vectorclock:le(MinVts, Vts) andalso vectorclock:le(Vts, MaxVts)
  end.

-spec get_clock_range(dcid(), vts_range()) -> clock_range().
get_clock_range(DcId, VtsRange) ->
  case VtsRange of
    {none, none} -> {none, none};
    {none, MaxVts} -> {none, vectorclock:get_clock_of_dc(DcId, MaxVts)};
    {MinVts, none} -> {vectorclock:get_clock_of_dc(DcId, MinVts), none};
    {MinVts, MaxVts} -> {vectorclock:get_clock_of_dc(DcId, MinVts), vectorclock:get_clock_of_dc(DcId, MaxVts)}
  end.

-spec is_in_clock_range(clock_time(), clock_range()) -> boolean().
is_in_clock_range(ClockTime, ClockRange) ->
  case ClockRange of
    {none, none} -> true;
    {none, MaxClockTime} -> ClockTime =< MaxClockTime;
    {MinClockTime, none} -> MinClockTime >= ClockTime;
    {MinClockTime, MaxClockTime} -> MinClockTime >= ClockTime andalso ClockTime =< MaxClockTime
  end.

-spec create_new_snapshot(key_struct()) -> snapshot().
create_new_snapshot(KeyStruct) ->
  Type = KeyStruct#key_struct.type,
  DefaultValue = gingko_utils:create_default_value(Type),
  #snapshot{key_struct = KeyStruct, commit_vts = vectorclock:new(), snapshot_vts = vectorclock:new(), value = DefaultValue}.

-spec create_cache_entry(snapshot()) -> cache_entry().
create_cache_entry(Snapshot) ->
  KeyStruct = Snapshot#snapshot.key_struct,
  CommitVts = Snapshot#snapshot.commit_vts,
  SnapshotVts = Snapshot#snapshot.snapshot_vts,
  Value = Snapshot#snapshot.value,
  #cache_entry{key_struct = KeyStruct, commit_vts = CommitVts, valid_vts = SnapshotVts, usage = #cache_usage{first_used = get_timestamp(), last_used = get_timestamp()}, blob = Value}.

-spec update_cache_usage(cache_entry(), boolean()) -> cache_entry().
update_cache_usage(CacheEntry, Used) ->
  CacheUsage = CacheEntry#cache_entry.usage,
  NewCacheUsage = case Used of
                    true ->
                      CacheUsage#cache_usage{used = true, last_used = get_timestamp(), times_used = CacheUsage#cache_usage.times_used + 1};
                    false -> CacheUsage#cache_usage{used = false}
                  end,
  CacheEntry#cache_entry{usage = NewCacheUsage}.

-spec create_snapshot_from_cache_entry(cache_entry()) -> snapshot().
create_snapshot_from_cache_entry(CacheEntry) ->
  KeyStruct = CacheEntry#cache_entry.key_struct,
  CommitVts = CacheEntry#cache_entry.commit_vts,
  SnapshotVts = CacheEntry#cache_entry.valid_vts,
  Value = CacheEntry#cache_entry.blob,
  #snapshot{key_struct = KeyStruct, commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}.

-spec create_default_value(type()) -> crdt() | none. %%TODO Consider other types
create_default_value(Type) ->
  IsCrdt = antidote_crdt:is_type(Type),
  case IsCrdt of
    true -> Type:new();
    false -> none
  end.

-spec is_system_operation(journal_entry(), OpType :: atom()) -> boolean().
is_system_operation(JournalEntry, OpType) ->
  Operation = JournalEntry#journal_entry.operation,
  is_record(Operation, system_operation) andalso Operation#system_operation.op_type == OpType.

-spec contains_system_operation([journal_entry()], system_operation_type()) -> boolean().
contains_system_operation(JournalEntries, SystemOpType) ->
  lists:any(fun(J) -> is_system_operation(J, SystemOpType) end, JournalEntries).

-spec is_update_of_keys(journal_entry(), [key_struct()] | all_keys) -> boolean().
is_update_of_keys(JournalEntry, KeyStructFilter) ->
  Operation = JournalEntry#journal_entry.operation,
  case is_record(Operation, object_operation) of
    true ->
      Operation#object_operation.op_type == update andalso (KeyStructFilter == all_keys orelse lists:member(Operation#object_operation.key_struct, KeyStructFilter));
    false -> false
  end.

-spec is_update_of_keys_or_commit(journal_entry(), [key_struct()] | all_keys) -> boolean().
is_update_of_keys_or_commit(JournalEntry, KeyStructFilter) ->
  is_update_of_keys(JournalEntry, KeyStructFilter) orelse is_system_operation(JournalEntry, commit_txn).

-spec is_update(journal_entry()) -> boolean().
is_update(JournalEntry) ->
  is_update_of_keys(JournalEntry, all_keys).

-spec get_keys_from_updates([journal_entry()]) -> [key_struct()].
get_keys_from_updates(JournalEntries) ->
  lists:filtermap(
    fun(J) ->
      case is_update(J) of
        true -> {true, J#journal_entry.operation#object_operation.key_struct};
        false -> false
      end
    end, JournalEntries).

-spec get_jsn_number(journal_entry()) -> non_neg_integer().
get_jsn_number(JournalEntry) ->
  JournalEntry#journal_entry.jsn#jsn.number.

-spec sort_by_jsn_number([journal_entry()]) -> [journal_entry()].
sort_by_jsn_number(JournalEntries) ->
  lists:sort(fun(J1, J2) -> get_jsn_number(J1) < get_jsn_number(J2) end, JournalEntries).

-spec get_latest_vts([journal_entry()]) -> vectorclock().
get_latest_vts(SortedJournalEntries) ->
  ReversedSortedJournalEntries = lists:reverse(SortedJournalEntries),
  LastJournalEntry = hd(ReversedSortedJournalEntries),
  LastJournalEntryWithVts = hd(
    lists:filter(
      fun(J) ->
        gingko_utils:is_system_operation(J, begin_txn) orelse gingko_utils:is_system_operation(J, commit_txn)
      end, ReversedSortedJournalEntries)),
  LastVts =
    case gingko_utils:is_system_operation(LastJournalEntryWithVts, begin_txn) of
      true ->
        LastJournalEntryWithVts#journal_entry.operation#system_operation.op_args#begin_txn_args.dependency_vts;
      false ->
        LastJournalEntryWithVts#journal_entry.operation#system_operation.op_args#commit_txn_args.commit_vts
    end,
  Timestamp = LastJournalEntry#journal_entry.rt_timestamp,
  vectorclock:set(gingko_utils:get_dcid(), Timestamp, LastVts).

-spec generate_downstream_op(key_struct(), txid(), type_op(), pid()) ->
  {ok, downstream_op()} | {error, atom()}.
generate_downstream_op(KeyStruct, TxId, TypeOp, CacheServerPid) ->
  %% TODO: Check if read can be omitted for some types as registers
  Type = KeyStruct#key_struct.type,
  NeedState = Type:require_state_downstream(TypeOp),
  Result =
    %% If state is needed to generate downstream, read it from the partition.
  case NeedState of
    true ->
      case gingko_log:perform_tx_read(KeyStruct, TxId, CacheServerPid) of
        {ok, S} ->
          S;
        {error, Reason} ->
          {error, {gen_downstream_read_failed, Reason}}
      end;
    false ->
      {ok, ignore} %Use a dummy value
  end,
  case Result of
    {error, R} ->
      {error, R}; %% {error, Reason} is returned here.
    Snapshot ->
      case Type of
        antidote_crdt_counter_b ->
          %% bcounter data-type. %%TODO bcounter!
          %%bcounter_mgr:generate_downstream(Key, TypeOp, Snapshot);
          {error, "BCounter not supported for now!"};
        _ ->
          Type:downstream(TypeOp, Snapshot)
      end
  end.