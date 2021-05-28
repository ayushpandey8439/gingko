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
-define(TABLE_CONCURRENCY, {read_concurrency,true}).
-record(cache_mgr_state, {}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CacheIdentifier) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, CacheIdentifier, []).

init(CacheIdentifier) ->
  logger:notice(#{
    action => "Starting Cache manager server",
    registered_as => ?MODULE
  }),
  ets:new(CacheIdentifier,[named_table,?TABLE_CONCURRENCY]),
  {ok, #cache_mgr_state{}}.

handle_call({put_in_cache,CacheIdentifier, Data},_From, State = #cache_mgr_state{}) ->
  Result = ets:insert(CacheIdentifier, {Data}),
  {reply, {ok, Result}, State};

handle_call({get_from_cache, CacheIdentifier, ObjectKey,Type,MaximumSnapshotTime }, _From, State = #cache_mgr_state{}) ->
  io:format("Check if the cache has the object ~n"),
  Reply = case ets:lookup(CacheIdentifier,ObjectKey) of
    [] ->
      io:format("Calling fill daemon ~n"),
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