-module(cache_daemon_vnode).
-include("gingko.hrl").
-behaviour(riak_core_vnode).
-ignore_xref([start_vnode/1]).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).



%% API
-export([get_from_cache/4, put_in_cache/1]).

-export([start_vnode/1, init/1, handle_command/3, terminate/2, is_empty/1, delete/1,
  handle_handoff_command/3, handoff_starting/2, handoff_cancelled/1, handoff_finished/2,
  handle_handoff_data/2, handle_overload_command/3, handle_overload_info/2,
  handle_coverage/4, handle_exit/3, encode_handoff_item/2]).


-record(cache_mgr_state, { partition, cacheidentifiers::list(), current_size:: integer(), cache_events:: dict:dict()}).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_from_cache(atom(), antidote_crdt:typ(), snapshot_time(), snapshot_time()) -> {ok, snapshot()}.
get_from_cache(ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime)->
  send_to_one(ObjectKey, {{get_from_cache, ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime}}).
  %gen_server:call(?CACHE_DAEMON, {get_from_cache, ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime}, infinity).

-spec put_in_cache({term(), antidote_crdt:typ(), snapshot_time()}) -> boolean().
put_in_cache({ObjectKey, Type, SnapshotTimestamp})->
  send_to_one(ObjectKey, {{put_in_cache, ObjectKey, Type, SnapshotTimestamp}}).
  %gen_server:call(?CACHE_DAEMON, {put_in_cache, Data}).



%%%===================================================================
%%% Spawning and vnode implementation
%%%===================================================================


-spec start_vnode(integer()) -> any().
start_vnode(I) ->
  riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->

  CacheIdentifiers = [],
  FinalIdentifiers =
    lists:foldl(fun(Level, CacheIdentifierList) ->
      CacheIdentifierID = list_to_atom(integer_to_list(Partition)++"gingko_cache"++integer_to_list(Level)),
      ets:new(CacheIdentifierID, [named_table, ?TABLE_CONCURRENCY]),
      lists:append(CacheIdentifierList, [{CacheIdentifierID, 500}] )
                end,
      CacheIdentifiers, lists:seq(1,3)),

  % This dictionary is only to collect metrics about the cache events.
  D = dict:new(),
  logger:notice(#{
    action => "Starting Cache Daemon VNode with " ++FinalIdentifiers ++" Cache Tables",
    registered_as => ?MODULE,
    pid => self()
  }),
  {ok, #cache_mgr_state{partition = Partition, cacheidentifiers = FinalIdentifiers, current_size = 0, cache_events = D}}.

handle_command(ping, _Sender, State = #cache_mgr_state{ partition = _Partition}) ->
  io:format("Received Ping. Responding"),
  {reply, {pong, node(), State#cache_mgr_state.partition}, State};
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
handoff_starting(TargetNode, State = #cache_mgr_state{partition = Partition}) ->
  logger:notice("handoff starting ~p: ~p", [Partition, TargetNode]),
  {true, State}.

handoff_cancelled(State = #cache_mgr_state{partition = Partition}) ->
  logger:notice("handoff cancelled ~p", [Partition]),
  {ok, State}.

handoff_finished(TargetNode, State = #cache_mgr_state{partition = Partition}) ->
  logger:notice("handoff finished ~p: ~p", [Partition, TargetNode]),
  {ok, State}.

handle_handoff_data(BinData, State) ->
  {reply, ok, State}.

encode_handoff_item(Key, Value) ->
  term_to_binary({Key, Value}).

is_empty(State = #cache_mgr_state{partition = _Partition}) ->
  {true, State}.

delete(State = #cache_mgr_state{partition = Partition}) ->
  logger:debug("Nothing to delete for partition ~p", [Partition]),
  {ok, State#cache_mgr_state{}}.

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


send_to_one(Key, Cmd) ->
  DocIdx = riak_core_util:chash_key({default_bucket, Key}),
  PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, gingko),
  [{IndexNode, _Type}] = PrefList,
  riak_core_vnode_master:sync_spawn_command(IndexNode, Cmd, gingko_vnode_master).