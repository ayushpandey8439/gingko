%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. May 2021 22:16
%%%-------------------------------------------------------------------
-module(cache_daemon).
-author("pandey").

-behaviour(gen_server).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).
% TODO Add types!!
-record(cache_mgr_state, {cacheidentifier,log_seq = #{}}).

%% API
-export([start_link/1, get_from_cache/3, put_in_cache/1, invalidate_cache_objects/1]).
%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

start_link(CacheIdentifier) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, CacheIdentifier, []).


-spec get_from_cache(atom(),atom(),vectorclock:vectorclock()) -> list().
get_from_cache(ObjectKey,Type,MaximumSnapshotTime)->
  gen_server:call(?SERVER,{get_from_cache, ObjectKey,Type,MaximumSnapshotTime }).

-spec put_in_cache({term(), antidote_crdt:typ(), vectorclock:vectorclock()}) -> boolean().
put_in_cache(Data)->
  gen_server:call(?SERVER,{put_in_cache, Data}).

invalidate_cache_objects(Keys) ->
  gen_server:call(?SERVER,{invalidate_objects, Keys}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

init(CacheIdentifier) ->
  logger:notice(#{
    action => "Starting Cache manager server",
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
  {reply, ok, State}.


handle_cast(_Request, State = #cache_mgr_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #cache_mgr_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #cache_mgr_state{}) ->
  ok.

code_change(_OldVsn, State = #cache_mgr_state{}, _Extra) ->
  {ok, State}.
