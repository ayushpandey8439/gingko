%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Aug 2021 10:21
%%%-------------------------------------------------------------------
-module(gingko_vnode).
-include("gingko.hrl").
-author("pandey").
-behaviour(riak_core_vnode).
%% API
-export([get_version/2]).
-export([start_vnode/1, init/1, handle_command/3, terminate/2, is_empty/1, delete/1,
  handle_handoff_command/3, handoff_starting/2, handoff_cancelled/1, handoff_finished/2,
  handle_handoff_data/2, handle_overload_command/3, handle_overload_info/2,
  handle_coverage/4, handle_exit/3, encode_handoff_item/2]).

-record(riak_core_fold_req_v2, {foldfun :: fun(), acc0 :: term(), forwardable :: boolean(), opts = [] :: list()}).
-record(state, {partition, kv_state}).

-spec start_vnode(integer()) -> any().
start_vnode(I) ->
  riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
  logger:notice(#{
    action => "Starting Gingko Vnode partition: ~p"++[Partition],
    registered_as => ?MODULE,
    pid => self()
  }),
  {ok, #state{partition = Partition, kv_state = #{}}}.

-spec get_version(key(), type()) -> {ok, snapshot()}.
get_version(Key, Type) ->
  get_version(Key, Type, ignore, ignore).
-spec get_version(key(), type(), snapshot_time(),snapshot_time()) -> {ok, snapshot()}.
get_version(Key, Type, MaximumSnapshot, MinimumSnapshot) ->
  ReqId = make_ref(),
  send_to_one(Key, {get_version, ReqId, {Key, Type, MaximumSnapshot, MinimumSnapshot}}).

send_to_one(Key, Cmd) ->
  DocIdx = riak_core_util:chash_key({default_bucket, Key}),
  PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, gingko_service),
  [{IndexNode, _Type}] = PrefList,
  riak_core_vnode_master:sync_spawn_command(IndexNode,
    Cmd,
    gingko_vnode_master).


handle_command({get_version, ReqId, ReqObject}, _Sender, State = #state{kv_state = KvState}) ->
  logger:notice("Received Request: ~p",[ReqObject]),
  {reply, {{request_id, ReqId}, {result, ok}}, State}.

%% -------------
%% HANDOFF
%% -------------

%% a vnode in the handoff lifecycle stage will not accept handle_commands anymore
%% instead every command is redirected to the handle_handoff_command implementations
%% for simplicity, we block every command except the fold handoff itself

%% every key in the vnode will be passed to this function
handle_handoff_command(#riak_core_fold_req_v2{foldfun = FoldFun, acc0 = Acc0},
    _Sender,
    State = #state{kv_state = KvState}) ->
  AllKeys = maps:keys(KvState),

  FoldKeys =
    fun (Key, AccIn) ->
      %% log
      logger:notice("Encoding key ~p for handoff", [Key]),

      %% get the value for the key
      Val = maps:get(Key, KvState),

      %% FoldFun uses encode_handoff_item to serialize the key-value pair and modify the handoff state Acc0
      Acc1 = FoldFun(Key, Val, AccIn),
      Acc1
    end,

  %% maps:fold can be used, too
  AccFinal = lists:foldl(FoldKeys, Acc0, AllKeys),

  %% kv store state does not change for this handoff implementation
  {reply, AccFinal, State};
handle_handoff_command(Message, _Sender, State) ->
  logger:warning("handoff command ~p, blocking until handoff is finished", [Message]),
  {reply, {error, processing_handoff}, State}.

handoff_starting(TargetNode, State = #state{partition = Partition}) ->
  logger:notice("handoff starting ~p: ~p", [Partition, TargetNode]),
  {true, State}.

handoff_cancelled(State = #state{partition = Partition}) ->
  logger:notice("handoff cancelled ~p", [Partition]),
  {ok, State}.

handoff_finished(TargetNode, State = #state{partition = Partition}) ->
  logger:notice("handoff finished ~p: ~p", [Partition, TargetNode]),
  {ok, State}.

handle_handoff_data(BinData, State = #state{kv_state = KvState}) ->
  TermData = binary_to_term(BinData),
  {Key, Value} = TermData,
  logger:notice("handoff data received for key ~p", [Key]),
  KvState1 = maps:put(Key, Value, KvState),
  {reply, ok, State#state{kv_state = KvState1}}.

encode_handoff_item(Key, Value) ->
  term_to_binary({Key, Value}).

is_empty(State = #state{kv_state = KvState, partition = _Partition}) ->
  IsEmpty = maps:size(KvState) == 0,
  {IsEmpty, State}.

delete(State = #state{partition = Partition, kv_state = #{}}) ->
  logger:debug("Nothing to delete for partition ~p", [Partition]),
  {ok, State#state{kv_state = #{}}};
delete(State = #state{partition = Partition, kv_state = KvState}) ->
  logger:info("delete ~p, ~p keys", [Partition, maps:size(KvState)]),
  {ok, State#state{kv_state = #{}}}.

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