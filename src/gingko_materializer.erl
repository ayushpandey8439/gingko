%% -------------------------------------------------------------------
%%
%% Copyright 2020, Kevin Bartik <k_bartik12@cs.uni-kl.de>
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
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(gingko_materializer).
-author("Kevin Bartik <k_bartik12@cs.uni-kl.de>").
-include("gingko.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([materialize_snapshot/3,
    materialize_tx_snapshot/2]).

-spec separate_commit_from_update_journal_entries([journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries(JournalEntryList) ->
    separate_commit_from_update_journal_entries(JournalEntryList, []).
-spec separate_commit_from_update_journal_entries([journal_entry()], [journal_entry()]) -> {journal_entry(), [journal_entry()]}.
separate_commit_from_update_journal_entries([CommitJournalEntry], JournalEntryList) ->
    {CommitJournalEntry, lists:reverse(JournalEntryList)};
separate_commit_from_update_journal_entries([UpdateJournalEntry | Rest], JournalEntryList) ->
    separate_commit_from_update_journal_entries(Rest, [UpdateJournalEntry | JournalEntryList]).

-spec get_committed_journal_entries_for_keys([journal_entry()], [key_struct()] | all_keys) -> [{journal_entry(), [journal_entry()]}].
%returns [{CommitJournalEntry, [UpdateJournalEntry]}]
get_committed_journal_entries_for_keys(SortedJournalEntryList, KeyStructFilter) ->
    FilteredJournalEntryList =
        lists:filter(
            fun(JournalEntry) ->
                gingko_utils:is_update_of_keys_or_commit(JournalEntry, KeyStructFilter)
            end, SortedJournalEntryList),
    TxIdToJournalEntryListMap = general_utils:group_by_map(fun(#journal_entry{tx_id = TxId}) ->
        TxId end, FilteredJournalEntryList),
    FilteredTxIdToJournalEntryListMap =
        maps:filter(
            fun(_TxId, JournalEntryList) ->
                gingko_utils:contains_journal_entry_type(JournalEntryList, commit_txn) andalso length(JournalEntryList) > 1 %Keep only transactions that were committed and have updates
            end, TxIdToJournalEntryListMap),
    SortedTxIdToJournalEntryListMap =
        maps:map(
            fun(_TxId, JournalEntryList) ->
                gingko_utils:sort_journal_entries_of_same_tx(JournalEntryList) %Last journal entry is commit then
            end, FilteredTxIdToJournalEntryListMap),
    CommitAndUpdateListTupleList =
        lists:map(
            fun({_TxId, JournalEntryList}) ->
                separate_commit_from_update_journal_entries(JournalEntryList) %{CommitJournalEntry, [UpdateJournalEntry]}
            end, maps:to_list(SortedTxIdToJournalEntryListMap)),
    SortedCommitToUpdateListTupleList = lists:sort(fun compare_commit_vts/2, CommitAndUpdateListTupleList),
    SortedCommitToUpdateListTupleList.

-spec compare_commit_vts({journal_entry(), [journal_entry()]}, {journal_entry(), [journal_entry()]}) -> boolean().
compare_commit_vts({#journal_entry{args = #commit_txn_args{commit_vts = CommitVts1}}, _JournalEntryList1}, {#journal_entry{args = #commit_txn_args{commit_vts = CommitVts2}}, _JournalEntryList2}) ->
    vectorclock:le(CommitVts1, CommitVts2).

-spec transform_to_update_payload(journal_entry(), journal_entry()) -> update_payload().
transform_to_update_payload(#journal_entry{args = #commit_txn_args{commit_vts = CommitVts}}, #journal_entry{args = #update_args{key_struct = KeyStruct, downstream_op = DownstreamOp}}) ->
    #update_payload{
        key_struct = KeyStruct,
        downstream_op = DownstreamOp,
        commit_vts = CommitVts,
        snapshot_vts = CommitVts
    }.

%%Relevant Journal Entries:
%% [{{journal_entry,8,dev1@KevinSB2,1594028250715554,{tx_id,1594028250687459,<0.306.0>},commit_txn,{commit_txn_args,#{dev1@KevinSB2 => 1594028250715439},{2,{tx_id,1594028250687459,<0.306.0>},1594028250715439}}},
%% [{journal_entry,6,dev1@KevinSB2,1594028250688791,{tx_id,1594028250687459,<0.306.0>},update,{update_args,{key_struct,antidote_key_static2,antidote_crdt_counter_pn},1,1}}]},
%% {{journal_entry,12,dev1@KevinSB2,1594028250774653,{tx_id,1594028250743531,<0.311.0>},commit_txn,{commit_txn_args,#{dev1@KevinSB2 => 1594028250774489},{3,{tx_id,1594028250743531,<0.311.0>},1594028250774489}}},
%% [{journal_entry,10,dev1@KevinSB2,1594028250744977,{tx_id,1594028250743531,<0.311.0>},update,{update_args,{key_struct,antidote_key_static2,antidote_crdt_counter_pn},1,1}}]}] |
%% Initial Snapshot: {snapshot,{key_struct,antidote_key_static2,antidote_crdt_counter_pn},#{dev1@KevinSB2 => 1594028250715439},#{dev1@KevinSB2 => 1594028250715439},1}
%%2020-07-06T11:37:30.798970+02:00 error: BenchTim

-spec materialize_snapshot(snapshot(), [journal_entry()], vectorclock()) -> {ok, snapshot()} | {error, reason()}.
materialize_snapshot(Snapshot = #snapshot{key_struct = KeyStruct, snapshot_vts = SnapshotVts}, JournalEntryList, DependencyVts) ->
    case gingko_utils:is_in_vts_range(SnapshotVts, {DependencyVts, none}) of
        true -> {ok, Snapshot};
        false ->
            CommittedJournalEntryList = get_committed_journal_entries_for_keys(JournalEntryList, [KeyStruct]),
            RelevantCommittedJournalEntryList =
                lists:filter(
                    fun({#journal_entry{args = #commit_txn_args{commit_vts = CommitVts}}, _UpdateJournalEntryList}) ->
                        vectorclock:gt(CommitVts, SnapshotVts) andalso gingko_utils:is_in_vts_range(CommitVts, {SnapshotVts, DependencyVts})
                    end, CommittedJournalEntryList),
            logger:error("Relevant Journal Entries: ~p | Initial Snapshot: ~p", [RelevantCommittedJournalEntryList, Snapshot]),
            UpdatePayloadListList =
                lists:map(
                    fun({CommitJournalEntry, UpdateJournalEntryList}) ->
                        lists:map(
                            fun(UpdateJournalEntry) ->
                                transform_to_update_payload(CommitJournalEntry, UpdateJournalEntry)
                            end, UpdateJournalEntryList)
                    end, RelevantCommittedJournalEntryList),
            UpdatePayloadList = lists:append(UpdatePayloadListList),
            {ok, NewSnapshot} = materialize_update_payload(Snapshot, UpdatePayloadList),
            {ok, NewSnapshot#snapshot{snapshot_vts = DependencyVts}}
    end.

-spec update_snapshot(snapshot(), downstream_op(), vectorclock(), vectorclock()) -> {ok, snapshot()} | {error, reason()}.
update_snapshot(Snapshot = #snapshot{value = SnapshotValue, key_struct = #key_struct{type = Type}}, DownstreamOp, CommitVts, SnapshotVts) ->
    IsCrdt = antidote_crdt:is_type(Type),
    case IsCrdt of
        true ->
            {ok, Value} = Type:update(DownstreamOp, SnapshotValue), %%TODO this will crash if downstream is bad
            {ok, Snapshot#snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts, value = Value}};
        false ->
            {error, {"Invalid Operation on Value", Snapshot, DownstreamOp, CommitVts, SnapshotVts}}
    end.

-spec materialize_update_payload(snapshot(), [update_payload()]) -> {ok, snapshot()} | {error, reason()}.
materialize_update_payload(Snapshot, []) ->
    {ok, Snapshot};
materialize_update_payload(Snapshot, [#update_payload{downstream_op = DownstreamOp, commit_vts = CommitVts, snapshot_vts = SnapshotVts} | Rest]) ->
    case update_snapshot(Snapshot, DownstreamOp, CommitVts, SnapshotVts) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_update_payload(Result, Rest)
    end.

-spec materialize_tx_snapshot(snapshot(), [downstream_op()]) -> {ok, snapshot()} | {error, reason()}.
materialize_tx_snapshot(Snapshot, []) ->
    {ok, Snapshot};
materialize_tx_snapshot(Snapshot = #snapshot{commit_vts = CommitVts, snapshot_vts = SnapshotVts}, [DownstreamOp | Rest]) ->
    case update_snapshot(Snapshot, DownstreamOp, CommitVts, SnapshotVts) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_tx_snapshot(Result, Rest)
    end.
