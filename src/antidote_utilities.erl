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

%% This file is combination of the dc_utilities and log_utilities
%% files from the antidote project https://github.com/AntidoteDB/antidote

-module(antidote_utilities).

-include("gingko.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
    get_my_dc_id/0,
    get_my_dc_nodes/0,
    call_vnode_sync/3,
    bcast_vnode_sync/2,
    partition_to_indexnode/1,
    call_vnode_async/3,
    get_all_partitions/0,
    get_all_partitions_nodes/0,
    bcast_vnode_async/2,
    get_my_partitions/0,
    ensure_all_vnodes_running/1,
    ensure_all_vnodes_running_master/1,
    get_partitions_num/0,
    check_registered/1,
    check_registered_global/1,
    call_vnode_async_with_key/3,
    call_vnode_sync_with_key/3,
    get_key_partition/1,
    get_preflist_from_key/1,
    get_my_node/1
]).

%% Returns the ID of the current DC.
-spec get_my_dc_id() -> dcid().
get_my_dc_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

%% Returns the list of all node addresses in the cluster.
-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

%% Returns the IndexNode tuple used by riak_core_vnode_master:command functions.
-spec partition_to_indexnode(partition_id()) -> {partition_id(), any()}.
partition_to_indexnode(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Node = riak_core_ring:index_owner(Ring, Partition),
    {Partition, Node}.

%% Returns a list of all partition indices in the cluster.
%% The partitions indices are 160-bit numbers that equally division the keyspace.
%% For example, for a cluster with 8 partitions, the indices would take following values:
%% 0, 1 * 2^157, 2 * 2^157, 3 * 2^157, 4 * 2^157, 5 * 2^157, 6 * 2^157, 7 * 2^157.
%% The partition numbers are erlang integers. To obtain the binary representation of the index,
%% use the inter_dc_txn:partition_to_bin/1 function.
-spec get_all_partitions() -> [partition_id()].
get_all_partitions() ->
    try
        {ok, Ring} = riak_core_ring_manager:get_my_ring(),
        CHash = riak_core_ring:chash(Ring),
        Nodes = chash:nodes(CHash),
        [I || {I, _} <- Nodes]
    catch
        _Ex:Res ->
            ?LOG_DEBUG("Error loading partition names: ~p, will retry", [Res]),
            get_all_partitions()
    end.

%% Returns a list of all partition indcies plus the node each
%% belongs to
-spec get_all_partitions_nodes() -> [{partition_id(), node()}].
get_all_partitions_nodes() ->
    try
        {ok, Ring} = riak_core_ring_manager:get_my_ring(),
        CHash = riak_core_ring:chash(Ring),
        chash:nodes(CHash)
    catch
        _Ex:Res ->
            ?LOG_DEBUG("Error loading partition-node names ~p, will retry", [Res]),
            get_all_partitions_nodes()
    end.

%% Returns the partition indices hosted by the local (caller) node.
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

%% Returns the number of partitions.
-spec get_partitions_num() -> non_neg_integer().
get_partitions_num() -> length(get_all_partitions()).

%% Sends the synchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_sync(partition_id(), atom(), any()) -> any().
call_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_spawn_command(partition_to_indexnode(Partition), Request, VMaster).

%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_async(partition_id(), atom(), any()) -> ok.
call_vnode_async(Partition, VMaster, Request) ->
    riak_core_vnode_master:command(partition_to_indexnode(Partition), Request, VMaster).

%% Sends the synchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_sync_with_key(key(), atom(), any()) -> any().
call_vnode_sync_with_key(Key, VMaster, Request) ->
    IndexNode = antidote_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_spawn_command(IndexNode, Request, VMaster).

%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_async_with_key(key(), atom(), any()) -> ok.
call_vnode_async_with_key(Key, VMaster, Request) ->
    IndexNode = antidote_utilities:get_key_partition(Key),
    riak_core_vnode_master:command(IndexNode, Request, VMaster).

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_vnode_sync(atom(), any()) -> [term()].
bcast_vnode_sync(VMaster, Request) ->
    %% TODO: check if pmap works like intended
    general_utils:parallel_map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_all_partitions()).

%% Sends the same (asynchronous) command to all vnodes of a given type.
-spec bcast_vnode_async(atom(), any()) -> ok.
bcast_vnode_async(VMaster, Request) ->
    general_utils:parallel_foreach(fun(P) -> {P, call_vnode_async(P, VMaster, Request)} end, get_all_partitions()).

%% Checks if all vnodes of a particular type are running.
%% The method uses riak_core methods to perform the check and was
%% shown to be unreliable in some very specific circumstances.
%% Use with caution.
-spec ensure_all_vnodes_running(atom()) -> ok.
ensure_all_vnodes_running(VnodeType) ->
    Partitions = get_partitions_num(),
    Running = length(riak_core_vnode_manager:all_vnodes(VnodeType)),
    case Partitions == Running of
        true -> ok;
        false ->
            ?LOG_DEBUG("Waiting for vnode ~p: required ~p, spawned ~p", [VnodeType, Partitions, Running]),
            %TODO: Extract into configuration constant
            timer:sleep(250),
            ensure_all_vnodes_running(VnodeType)
    end.

%% Internal function that loops until a given vnode type is running
-spec bcast_vnode_check_up(atom(), {hello}, [partition_id()]) -> ok.
bcast_vnode_check_up(_VMaster, _Request, []) ->
    ok;
bcast_vnode_check_up(VMaster, Request, [P | Rest]) ->
    Err = try
              case call_vnode_sync(P, VMaster, Request) of
                  ok ->
                      false;
                  _Msg ->
                      true
              end
          catch
              _Ex:_Res ->
                  true
          end,
    case Err of
        true ->
            ?LOG_DEBUG("Vnode not up retrying, ~p, ~p", [VMaster, P]),
            %TODO: Extract into configuration constant
            timer:sleep(1000),
            bcast_vnode_check_up(VMaster, Request, [P | Rest]);
        false ->
            bcast_vnode_check_up(VMaster, Request, Rest)
    end.

%% Loops until all vnodes of a given type are running on all
%% nodes in the cluster
-spec ensure_all_vnodes_running_master(atom()) -> ok.
ensure_all_vnodes_running_master(VnodeType) ->
    check_registered(VnodeType),
    bcast_vnode_check_up(VnodeType, {hello}, get_all_partitions()).

%% Loops until a process with the given name is registered locally
-spec check_registered(atom()) -> ok.
check_registered(Name) ->
    case whereis(Name) of
        undefined ->
            ?LOG_DEBUG("Wait for ~p to register", [Name]),
            timer:sleep(100),
            check_registered(Name);
        _ ->
            ok
    end.

%% Loops until a process with the given name is registered globally
-spec check_registered_global(atom()) -> ok.
check_registered_global(Name) ->
    case global:whereis_name(Name) of
        undefined ->
            timer:sleep(100),
            check_registered_global(Name);
        _ ->
            ok
    end.

%% @doc get_key_partition returns the most probable node where a given
%%      key's logfile will be located.
-spec get_key_partition(key()) -> index_node().
get_key_partition(Key) ->
    IndexNode = hd(get_preflist_from_key(Key)),
    IndexNode.

%% @doc get_preflist_from_key returns a preference list where a given
%%      key's logfile will be located.
-spec get_preflist_from_key(key()) -> preflist().
get_preflist_from_key(Key) ->
    ConvertedKey = convert_key(Key),
    get_primaries_preflist(ConvertedKey).

%% @doc get_primaries_preflist returns the preflist with the primary
%%      vnodes. No matter they are up or down.
%%      Input:  A hashed key
%%      Return: The primaries preflist
%%
-spec get_primaries_preflist(non_neg_integer()) -> preflist().
get_primaries_preflist(Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {NumPartitions, ListOfPartitions} = riak_core_ring:chash(Ring),
    Pos = Key rem NumPartitions + 1,
    {Index, Node} = lists:nth(Pos, ListOfPartitions),
    [{Index, Node}].

-spec get_my_node(partition_id()) -> node().
get_my_node(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:index_owner(Ring, Partition).

%% @doc Convert key. If the key is integer(or integer in form of binary),
%% directly use it to get the partition. If it is not integer, convert it
%% to integer using hash.
-spec convert_key(key()) -> non_neg_integer().
convert_key(Key) ->
    case is_binary(Key) of
        true ->
            KeyInt = (catch list_to_integer(binary_to_list(Key))),
            case is_integer(KeyInt) of
                true -> abs(KeyInt);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, Key}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end;
        false ->
            case is_integer(Key) of
                true ->
                    abs(Key);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end
    end.
