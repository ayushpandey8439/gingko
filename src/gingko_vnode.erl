-module(gingko_vnode).

-behaviour(riak_core_vnode).

%% API
-export([start_vnode/1, init/1, handle_command/3, terminate/2, is_empty/1, delete/1,
    handle_handoff_command/3, handoff_starting/2, handoff_cancelled/1, handoff_finished/2,
    handle_handoff_data/2, handle_overload_command/3, handle_overload_info/2,
    handle_coverage/4, handle_exit/3, encode_handoff_item/2]).

-record(state, {partition, kv_state}).

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition = Partition}}.

handle_command(ping, _Sender, State = #state{ partition = _Partition}) ->
    io:format("Received Ping. Responding"),
    {reply, {pong, node(), State#state.partition}, State};
handle_command({get_version, Key,Type,MinimumSnapshotTime,MaximumSnapshotTime}, _Sender,State) ->
    Result = cache_daemon:get_from_cache(Key,Type,MinimumSnapshotTime,MaximumSnapshotTime),
    {reply, Result, State};
handle_command(Message, _Sender, State) ->
    logger:warning("unhandled_command ~p", [Message]),
    {noreply, State}.

%% -------------
%% HANDOFF
%% -------------

%% a vnode in the handoff lifecycle stage will not accept handle_commands anymore
%% instead every command is redirected to the handle_handoff_command implementations
%% for simplicity, we block every command except the fold handoff itself

%% every key in the vnode will be passed to this function
handle_handoff_command(Message, _Sender, State) ->
    logger:warning("handoff command ~p, ignoring", [Message]),
    {noreply, State}.
handoff_starting(TargetNode, State = #state{partition = Partition}) ->
    logger:notice("handoff starting ~p: ~p", [Partition, TargetNode]),
    {true, State}.

handoff_cancelled(State = #state{partition = Partition}) ->
    logger:notice("handoff cancelled ~p", [Partition]),
    {ok, State}.

handoff_finished(TargetNode, State = #state{partition = Partition}) ->
    logger:notice("handoff finished ~p: ~p", [Partition, TargetNode]),
    {ok, State}.

handle_handoff_data(BinData, State) ->
    {reply, ok, State}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

is_empty(State = #state{partition = _Partition}) ->
    {true, State}.

delete(State = #state{partition = Partition}) ->
    logger:debug("Nothing to delete for partition ~p", [Partition]),
    {ok, State#state{}}.

%% -------------
%% Not needed / not implemented
%% -------------

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.