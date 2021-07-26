%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache_daemon).
-include("gingko.hrl").

-behaviour(gen_server).

-export([start_link/3]).
-export([get_from_cache/4, put_in_cache/1, invalidate_cache_objects/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).


-record(cache_mgr_state, {cacheidentifiers::#{}}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_from_cache(atom(), atom(), snapshot_time(), snapshot_time()) -> {ok, snapshot()}.
get_from_cache(ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime)->
  gen_server:call(?CACHE_DAEMON, {get_from_cache, ObjectKey, Type, MinimumSnapshotTime, MaximumSnapshotTime}, infinity).

-spec put_in_cache({term(), antidote_crdt:typ(), snapshot_time()}) -> boolean().
put_in_cache(Data)->
  gen_server:call(?CACHE_DAEMON, {put_in_cache, Data}).
-spec invalidate_cache_objects(list()) -> ok.
invalidate_cache_objects(Keys) ->
  gen_server:call(?CACHE_DAEMON, {invalidate_objects, Keys}).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CacheIdentifier, Levels, SegmentSize) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {CacheIdentifier, Levels, SegmentSize}, []).

init({CacheIdentifier, Levels, SegmentSize}) ->
  logger:notice(#{
    action => "Starting Cache Daemon with "++ integer_to_list(Levels) ++ " Segments of "++ integer_to_list(SegmentSize)++" objects each",
    registered_as => ?MODULE,
    pid => self()
  }),
  CacheIdentifiers = [],
  FinalIdentifiers =
    lists:foldl(fun(Level, CacheIdentifierList) ->
      CacheIdentifierID = list_to_atom(atom_to_list(CacheIdentifier)++integer_to_list(Level)),
      ets:new(CacheIdentifierID, [named_table, ?TABLE_CONCURRENCY]),
      lists:append(CacheIdentifierList, [{CacheIdentifierID, SegmentSize}] )
                end,
      CacheIdentifiers, lists:seq(1,Levels)),
  {ok, #cache_mgr_state{cacheidentifiers = FinalIdentifiers}}.
  
handle_call({put_in_cache, Data}, _From, State = #cache_mgr_state{}) ->
  Result = cacheInsert(State#cache_mgr_state.cacheidentifiers, Data),
  {reply, {ok, Result}, State};

handle_call({get_from_cache, ObjectKey, Type, MinimumSnapshotTime,MaximumSnapshotTime}, _From, State = #cache_mgr_state{}) ->
  Reply =
    case cacheLookup(State#cache_mgr_state.cacheidentifiers, ObjectKey) of
    {error, not_exist} ->
      logger:debug("Cache Miss: Going to the log to materialize."),
      % TODO: Go to the checkpoint store and get the last stable version and build on top of it.
      {SnapshotTime, MaterializedObject} = fill_daemon:build(ObjectKey, Type, ignore, MaximumSnapshotTime),
      UpdatedIdentifiers = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, SnapshotTime, MaterializedObject}),
      {ObjectKey, Type, MaterializedObject};
    {ok, {ObjectKey, Type, SnapshotTime, MaterializedObject}} ->
      SnapshotTimeLowerMinTime = clock_comparision:check_min_time_gt(MinimumSnapshotTime, SnapshotTime),
      SnapshotTimeHigherMaxTime  = clock_comparision:check_max_time_le(MaximumSnapshotTime,SnapshotTime),
      UpdatedMaterialization = if
         SnapshotTimeLowerMinTime == true ->
           logger:debug("Cache hit, Object in the cache is stale. ~p ~p",[SnapshotTime, MaterializedObject]),
           {Timestamp, Materialization} = fill_daemon:build(ObjectKey, Type, MaterializedObject, SnapshotTime, MaximumSnapshotTime),
           % Insert the element in the cache for later reads.
           UpdatedIdentifiers = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, Timestamp, Materialization}),
           Materialization;
         SnapshotTimeHigherMaxTime == true ->
           logger:debug("Cache hit, Object in the cache has a timestamp higher than the required minimum."),
           {Timestamp, Materialization} = fill_daemon:build(ObjectKey, Type, ignore, MaximumSnapshotTime),
           UpdatedIdentifiers = cacheInsert(State#cache_mgr_state.cacheidentifiers, {ObjectKey, Type, Timestamp, Materialization}),
           Materialization;
         true ->
           logger:debug("Cache hit, Object is within bounds"),
           UpdatedIdentifiers = State#cache_mgr_state.cacheidentifiers,
           MaterializedObject
      end,
      {ObjectKey, Type, UpdatedMaterialization}
  end,
  {reply, {ok, Reply}, State#cache_mgr_state{cacheidentifiers = UpdatedIdentifiers}};

handle_call({invalidate_objects, Keys}, _From, State = #cache_mgr_state{}) ->
  {CacheStore, _Size} = lists:nth(1, State#cache_mgr_state.cacheidentifiers),
  lists:foreach(fun(ObjectKey) -> ets:delete(CacheStore, ObjectKey) end, Keys),
  %% The delete here can also be replaced with tombstones.
  %% Then the reads will have to check for staleness adding an extra operation.
  %% This will have consequences when the reads are disproportionately high compared to the commits and writes.
  {reply, ok, State}.


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

cacheInsert(CacheIdentifiers, Data) ->
  {CacheStore, MaxSize} = lists:nth(1, CacheIdentifiers),
  TableSize = ets:info(CacheStore, size),
  ets:insert(CacheStore, Data),
  case TableSize >= MaxSize of
    false -> CacheIdentifiers;
    true -> garbageCollect(CacheIdentifiers)
  end.


garbageCollect([])->
  [];
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