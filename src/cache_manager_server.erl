%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache_manager).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).
-export([get_from_cache/4,put_in_cache/2]).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency,true}).
-record(cache_mgr_state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CacheIdentifier) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, CacheIdentifier, []).

init(CacheIdentifier) ->
  ets:new(CacheIdentifier,[named_table,?TABLE_CONCURRENCY]),
  {ok, #cache_mgr_state{}}.

handle_call({put_in_cache,CacheIdentifier, Data},_From, State = #cache_mgr_state{}) ->
  ets:insert(CacheIdentifier, {Data});
handle_call({get_from_cache, CacheIdentifier, ObjectKey,Type,MaximumSnapshotTime }, _From, State = #cache_mgr_state{}) ->
  Reply = case ets:lookup(CacheIdentifier,ObjectKey) of
    [] ->
      fill_daemon:build(ObjectKey,Type,MaximumSnapshotTime); %% Maximum time not used yet.
    [CacheObject] ->
      {ok,CacheObject}
  end,
  {reply, {ok, Reply}, State}.

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



-spec get_from_cache(term(),atom(),atom(),vectorclock:vectorclock()) -> list().
get_from_cache(CacheIdentifier, ObjectKey,Type,MaximumSnapshotTime)->
  gen_server:call(self(),{get_from_cache, CacheIdentifier, ObjectKey,Type,MaximumSnapshotTime }).

-spec put_in_cache(term(),{term(), antidote_crdt:typ(), vectorclock:vectorclock()}) -> boolean().
put_in_cache(CacheIdentifier,Data)->
  gen_server:call(self(),{put_in_cache,CacheIdentifier, Data}).
