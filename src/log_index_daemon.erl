%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(log_index_daemon).
-include("gingko.hrl").
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-export([get_continuation/2, add_to_index/3]).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).

-record(log_index_daemon_state, {indexidentifier :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================
get_continuation(Key, SnapshotTimestamp) ->
  gen_server:call(?LOG_INDEX_DAEMON, {get_continuation, Key, SnapshotTimestamp}).

add_to_index(Key, SnapshotTimestamp, Continuation) ->
  gen_server:call(?LOG_INDEX_DAEMON, {index, Key, SnapshotTimestamp, Continuation}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(IndexIdentier) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, IndexIdentier, []).

init(IndexIdentifier) ->
  logger:notice(#{
    action => "Starting log index daemon",
    registered_as => ?MODULE,
    pid => self()
  }),
  ets:new(IndexIdentifier, [named_table, ?TABLE_CONCURRENCY]),
  {ok, #log_index_daemon_state{indexidentifier = IndexIdentifier}}.

handle_call({get_continuation, Key, SnapshotTimestamp}, _From, State = #log_index_daemon_state{}) ->
  Continuation =
    case ets:lookup(State#log_index_daemon_state.indexidentifier, {Key, SnapshotTimestamp}) of
      [] -> start;
      [List] -> List

    end,
  {reply, {ok, Continuation} , State};

handle_call({index, Key, SnapshotTimestamp, Continuation}, _From, State = #log_index_daemon_state{}) ->
  ets:insert(State#log_index_daemon_state.indexidentifier, {{Key, SnapshotTimestamp}, Continuation}),
  {reply, ok, State}.

handle_cast(_Request, State = #log_index_daemon_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #log_index_daemon_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #log_index_daemon_state{}) ->
  ok.

code_change(_OldVsn, State = #log_index_daemon_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
