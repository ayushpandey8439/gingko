%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache_daemon).
-include("gingko.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([get_from_cache/3,put_in_cache/1,invalidate_cache_objects/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).
-record(cache_mgr_state, {cacheidentifier,log_seq = #{}}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_from_cache(atom(),atom(),vectorclock:vectorclock()) -> {ok,snapshot()}.
get_from_cache(ObjectKey,Type,MaximumSnapshotTime)->
  gen_server:call(?CACHE_DAEMON,{get_from_cache, ObjectKey,Type,MaximumSnapshotTime }).

-spec put_in_cache({term(), antidote_crdt:typ(), vectorclock:vectorclock()}) -> boolean().
put_in_cache(Data)->
  gen_server:call(?CACHE_DAEMON,{put_in_cache, Data}).
-spec invalidate_cache_objects(list()) -> ok.
invalidate_cache_objects(Keys) ->
  gen_server:call(?CACHE_DAEMON,{invalidate_objects, Keys}).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CacheIdentifier) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, CacheIdentifier, []).

init(CacheIdentifier) ->
  logger:notice(#{
    action => "Starting Cache Daemon",
    registered_as => ?MODULE,
    pid => self()
  }),
  ets:new(CacheIdentifier,[named_table,?TABLE_CONCURRENCY]),
  {ok, #cache_mgr_state{cacheidentifier = CacheIdentifier}}.

handle_call({put_in_cache, Data},_From, State = #cache_mgr_state{}) ->
  Result = ets:insert(State#cache_mgr_state.cacheidentifier, {Data}),
  {reply, {ok, Result}, State};

handle_call({get_from_cache, ObjectKey,Type,MaximumSnapshotTime }, _From, State = #cache_mgr_state{}) ->
  Reply = case ets:lookup(State#cache_mgr_state.cacheidentifier,ObjectKey) of
    [] ->
      {_LastLSN, MaterializedObject} = fill_daemon:build(ObjectKey,Type,MaximumSnapshotTime),
      ets:insert(State#cache_mgr_state.cacheidentifier,{ObjectKey,Type,MaterializedObject}),
      {ObjectKey,Type,MaterializedObject};
    [CacheObject] ->
      %% TODO Check if there have been new operations on the object beyond the clock timestamp atm. If yes, rematerialise it and store it in the cache.
      CacheObject
  end,
  {reply, {ok, Reply}, State};

handle_call({invalidate_objects, Keys}, _From, State = #cache_mgr_state{}) ->
  lists:foreach(fun(ObjectKey) -> ets:delete(State#cache_mgr_state.cacheidentifier,ObjectKey) end, Keys), 
  %% The delete here can also be replaced with tombstones but then the reads will have to check for staleness adding an extra operation.
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
%%% Unit Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").
