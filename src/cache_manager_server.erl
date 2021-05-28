%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache_manager_server).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).
-record(cache_mgr_state, {cacheidentifier}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CacheIdentifier) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, CacheIdentifier, []).

init(CacheIdentifier) ->
  logger:notice(#{
    action => "Starting Cache manager server",
    registered_as => ?MODULE,
    pid => self()
  }),
  ets:new(CacheIdentifier,[named_table,?TABLE_CONCURRENCY]),
  {ok, #cache_mgr_state{cacheidentifier = CacheIdentifier}}.

handle_call(ping,_From, State = #cache_mgr_state{}) ->
  {reply, {ok, hello}, State};
handle_call({put_in_cache, Data},_From, State = #cache_mgr_state{}) ->
  Result = ets:insert(State#cache_mgr_state.cacheidentifier, {Data}),
  {reply, {ok, Result}, State};

handle_call({get_from_cache, ObjectKey,Type,MaximumSnapshotTime }, _From, State = #cache_mgr_state{}) ->
  Reply = case ets:lookup(State#cache_mgr_state.cacheidentifier,ObjectKey) of
    [] ->
      MaterializedObject = fill_daemon:build(ObjectKey,Type,MaximumSnapshotTime),
      ets:insert(State#cache_mgr_state.cacheidentifier,{ObjectKey,Type,MaterializedObject}),
      {ObjectKey,Type,MaterializedObject};
    [CacheObject] ->
      CacheObject
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