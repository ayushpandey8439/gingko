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

-define(SERVER, ?MODULE).
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

  % This dictionary is only to collect metrics about the cache events.
  D = dict:new(),
  {ok, #cache_mgr_state{cacheidentifiers = FinalIdentifiers, current_size = 0, cache_events = D}}.
  
handle_call({put_in_cache, Data}, _From, State = #cache_mgr_state{current_size = Size}) ->
  Result = cacheInsert(State#cache_mgr_state.cacheidentifiers, Data, Size),
  {reply, {ok, Result}, State#cache_mgr_state{current_size = Size+1}};

handle_call({get_from_cache, TxId, ObjectKey, Type, MinimumSnapshotTime,MaximumSnapshotTime, Partition}, _From, State = #cache_mgr_state{current_size = Size, cache_events = Events}) ->
  {ReplyMaterializedObject, ReplySnapshotTime, NewEvents} =
    case cacheLookup(State#cache_mgr_state.cacheidentifiers, ObjectKey) of
    {error, not_exist} ->
      EventsUpdated = dict:update_counter(misses, 1, Events),
      % TODO: Go to the checkpoint store and get the last stable version and build on top of it.
      {MaterializationSnapshotTime, MaterializedObject} = fill_daemon:build(TxId, ObjectKey, Type, ignore, MaximumSnapshotTime,Partition),
      logger:error("MaterializationSnapshot time for first insert to cache is ~p",[MaterializationSnapshotTime]),
      {UpdatedIdentifiers, NewSize} = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, MaterializationSnapshotTime, MaterializedObject}, Size),
      {MaterializedObject, MaterializationSnapshotTime,EventsUpdated};
    {ok, {ObjectKey, Type, CacheSnapshotTime, MaterializedObject}} ->
      logger:error("Cached version with timestamp ~p Min required = ~p Max required = ~p",[CacheSnapshotTime, MinimumSnapshotTime, MaximumSnapshotTime]),
      SnapshotTimeLowerMinTime = clock_comparision:check_min_time_gt(MinimumSnapshotTime, CacheSnapshotTime),
      SnapshotTimeHigherMaxTime  = clock_comparision:check_max_time_le(MaximumSnapshotTime, CacheSnapshotTime),
      {MaterializationTimestamp, UpdatedMaterialization, NewEventsInternal} = if
         SnapshotTimeHigherMaxTime == true ->
           EventsUpdated = dict:update_counter(comp_rebuilds, 1, Events),
           logger:debug("Cache hit, Object in the cache has a timestamp higher than the required minimum. ~p ~p ~p",[CacheSnapshotTime, MinimumSnapshotTime, MaximumSnapshotTime]),
           {Timestamp, Materialization} = fill_daemon:build(TxId, ObjectKey, Type, ignore, MaximumSnapshotTime, Partition),
           {UpdatedIdentifiers, NewSize} = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, Timestamp, Materialization}, Size),
           {Timestamp, Materialization, EventsUpdated};
         SnapshotTimeLowerMinTime == true ->
           EventsUpdated = dict:update_counter(stales, 1, Events),
           logger:debug("Cache hit, Object in the cache is stale."),
           {Timestamp, Materialization} = fill_daemon:build(TxId, ObjectKey, Type, MaterializedObject, CacheSnapshotTime, MaximumSnapshotTime,Partition),
           % Insert the element in the cache for later reads.
           {UpdatedIdentifiers, NewSize} = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, Timestamp, Materialization}, Size),
           {Timestamp, Materialization, EventsUpdated};
         true ->
           EventsUpdated = dict:update_counter(in_bounds, 1, Events),
           logger:debug("Cache hit, Object is within bounds ~p ~p ~p",[CacheSnapshotTime, MinimumSnapshotTime, MaximumSnapshotTime]),
           UpdatedIdentifiers = State#cache_mgr_state.cacheidentifiers,
           {CacheSnapshotTime, MaterializedObject, EventsUpdated}
      end,
      {UpdatedMaterialization, MaterializationTimestamp, NewEventsInternal}
  end,
  {reply, {ok, {ObjectKey, Type, ReplyMaterializedObject, ReplySnapshotTime}}, State#cache_mgr_state{cacheidentifiers = UpdatedIdentifiers, cache_events = NewEvents}};

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