%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache_daemon).
-include("gingko.hrl").

-behaviour(gen_server).

-export([start_link/4]).
-export([get_from_cache/6, put_in_cache/1, invalidate_cache_objects/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(TABLE_CONCURRENCY, {read_concurrency, true}).


-record(cache_mgr_state, {cacheidentifiers::list(), current_size:: integer(), cache_events:: dict:dict()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_from_cache(term(),atom(), antidote_crdt:typ(), snapshot_time(), snapshot_time(), integer()) -> {ok, snapshot()}.
get_from_cache(TxId, ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime, Partition)->
  gen_server:call(list_to_atom(atom_to_list(?CACHE_DAEMON)++integer_to_list(Partition)), {get_from_cache, TxId,  ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime, Partition}, infinity).

-spec put_in_cache({term(), antidote_crdt:typ(), snapshot_time()}) -> boolean().
put_in_cache(Data)->
  gen_server:call(?CACHE_DAEMON, {put_in_cache, Data}).
-spec invalidate_cache_objects(list()) -> ok.
invalidate_cache_objects(Keys) ->
  gen_server:call(?CACHE_DAEMON, {invalidate_objects, Keys}).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CacheIdentifier, Levels, SegmentSize, Partition) ->
  gen_server:start_link({local, list_to_atom(atom_to_list(?CACHE_DAEMON)++integer_to_list(Partition))}, ?MODULE, {CacheIdentifier, Levels, SegmentSize, Partition}, []).

init({CacheIdentifier, Levels, SegmentSize, Partition}) ->
  logger:notice(#{
    action => "Starting Cache Daemon with "++ integer_to_list(Levels) ++ " Segments of "++ integer_to_list(SegmentSize)++" objects each",
    registered_as => ?MODULE,
    pid => self()
  }),
  CacheIdentifiers = [],
  FinalIdentifiers =
    lists:foldl(fun(Level, CacheIdentifierList) ->
      CacheIdentifierID = list_to_atom(integer_to_list(Partition)++atom_to_list(CacheIdentifier)++integer_to_list(Level)),
      ets:new(CacheIdentifierID, [named_table, ?TABLE_CONCURRENCY]),
      lists:append(CacheIdentifierList, [{CacheIdentifierID, SegmentSize}] )
                end,
      CacheIdentifiers, lists:seq(1,Levels)),

  % This dictionary is only to collect metrics about the cache events.Hi @schimpfa , I have written sopme
  D = dict:new(),
  {ok, #cache_mgr_state{cacheidentifiers = FinalIdentifiers, current_size = 0, cache_events = D}}.

handle_call({put_in_cache, Data}, _From, State = #cache_mgr_state{current_size = Size}) ->
  Result = cacheInsert(State#cache_mgr_state.cacheidentifiers, Data, Size),
  {reply, {ok, Result}, State#cache_mgr_state{current_size = Size+1}};

handle_call({get_from_cache, TxId, ObjectKey, Type, MinimumSnapshotTime,MaximumSnapshotTime, Partition}, _From, State = #cache_mgr_state{current_size = Size}) ->
  {ReplyMaterializedObject, ReplySnapshotTime} = case cacheLookup(State#cache_mgr_state.cacheidentifiers, ObjectKey) of
    {error, not_exist} ->
      logger:debug("Cache miss"),
      {ReturnObject, CacheObject, CacheTimestamp} = case checkpoint_daemon:get_checkpoint(ObjectKey, MinimumSnapshotTime, Partition) of
        {exact_match, MatchedSnapshotTime, MatchedSnapshot} ->
          logger:debug("Exact checkpoint"),
          {MatchedSnapshot,MatchedSnapshot, MatchedSnapshotTime};
        {non_exact_match, ClosestSnapshotTime, ClosestSnapshot} ->
          logger:debug("Close checkpoint"),
          {MaterializationSnapshotTime, StableMaterialization,  InteractiveMaterialization} = fill_daemon:build(TxId, ObjectKey, Type, ClosestSnapshot, ClosestSnapshotTime, MaximumSnapshotTime,Partition),
          {InteractiveMaterialization,StableMaterialization, MaterializationSnapshotTime};
        {error, not_exist} ->
          logger:debug("Checkpoint miss"),
          {MaterializationSnapshotTime, StableMaterialization,  InteractiveMaterialization} = fill_daemon:build(TxId, ObjectKey, Type, ignore, MaximumSnapshotTime,Partition),
          {InteractiveMaterialization, StableMaterialization, MaterializationSnapshotTime}
      end,
      {UpdatedIdentifiers, _NewSize} = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, CacheTimestamp, CacheObject}, Size),
      {ReturnObject, CacheTimestamp};
    {ok, {ObjectKey, Type, CacheSnapshotTime, MaterializedObject}} ->
      SnapshotTimeLowerMinTime = clock_comparision:check_min_time_gt(MinimumSnapshotTime, CacheSnapshotTime),
      SnapshotTimeHigherMaxTime  = clock_comparision:check_max_time_le(MaximumSnapshotTime, CacheSnapshotTime),
      {MaterializationTimestamp, UpdatedMaterialization} =
        if
         SnapshotTimeHigherMaxTime == true ->
           {Timestamp, StableMaterialization, InteractiveMaterialization} = fill_daemon:build(TxId, ObjectKey, Type, ignore, MaximumSnapshotTime, Partition),
           {UpdatedIdentifiers, _NewSize} = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, Timestamp, StableMaterialization}, Size),
           {Timestamp, InteractiveMaterialization};
         SnapshotTimeLowerMinTime == true ->
           {Timestamp, StableMaterialization, InteractiveMaterialization} = fill_daemon:build(TxId, ObjectKey, Type, MaterializedObject, CacheSnapshotTime, MaximumSnapshotTime,Partition),
           % Insert the element in the cache for later reads.
           {UpdatedIdentifiers, _NewSize} = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, Timestamp, StableMaterialization}, Size),
           {Timestamp, InteractiveMaterialization};
         true ->
           UpdatedIdentifiers = State#cache_mgr_state.cacheidentifiers,
           {CacheSnapshotTime, MaterializedObject}
      end,
      {UpdatedMaterialization, MaterializationTimestamp}
  end,
  {reply, {ok, {ObjectKey, Type, ReplyMaterializedObject, ReplySnapshotTime}}, State#cache_mgr_state{cacheidentifiers = UpdatedIdentifiers}};

handle_call({invalidate_objects, Keys}, _From, State = #cache_mgr_state{}) ->
  {CacheStore, _Size} = lists:nth(1, State#cache_mgr_state.cacheidentifiers),
  lists:foreach(fun(ObjectKey) -> ets:delete(CacheStore, ObjectKey) end, Keys),
  %% The delete here can also be replaced with tombstones.
  %% Then the reads will have to check for staleness adding an extra operation.
  %% This will have consequences when the reads are disproportionately high compared to the commits and writes.
  {reply, ok, State};

handle_call({get_event_stats}, _From, State = #cache_mgr_state{cache_events = Events}) ->
  {reply, dict:to_list(Events), State}.


handle_cast(_Request, State = #cache_mgr_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #cache_mgr_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #cache_mgr_state{}) ->
  ok.

code_change(_OldVsn, State = #cache_mgr_state{}, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
cacheLookup([],_Key) ->
  {error, not_exist};
cacheLookup([{CacheStore,_Size}| CacheIdentifiers], Key) ->
  case ets:lookup(CacheStore, Key) of
    [] -> cacheLookup(CacheIdentifiers, Key);
    [Object] -> {ok, Object}
  end.

cacheInsert(CacheIdentifiers, Data, Size) ->
  {CacheStore, MaxSize} = lists:nth(1, CacheIdentifiers),
  ets:insert(CacheStore, Data),
  case Size >= MaxSize of
    false -> {CacheIdentifiers, Size+1};
    true ->
      case ets:info(CacheStore, size) >= MaxSize of
        false -> {CacheIdentifiers, Size+1};
        true -> {garbageCollect(CacheIdentifiers), 0}
      end
  end.

-spec garbageCollect(list()) -> list().
garbageCollect(CacheIdentifiers) ->
  logger:debug("Initiating Garbage Collection"),
  {LastSegment, Size} = lists:last(CacheIdentifiers),
  ets:delete(LastSegment),
  ets:new(LastSegment, [named_table, ?TABLE_CONCURRENCY]),
  SubList = lists:droplast(CacheIdentifiers),
  UpdatedCacheIdentifiers = lists:append([{LastSegment,Size}],SubList),
  logger:debug("New CacheIdentifier List is ~p ~n",[UpdatedCacheIdentifiers]),
  UpdatedCacheIdentifiers.

%%%===================================================================
%%% Unit Tests
%%%===================================================================