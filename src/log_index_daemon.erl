%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(log_index_daemon).
-include("gingko.hrl").
-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-export([get_continuation/3, add_to_index/4]).

-define(SERVER, ?MODULE).
-define(TABLE_CONCURRENCY, {read_concurrency, true}).

-record(log_index_daemon_state, {indexidentifier :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================
get_continuation(Key, Timestamp, Partition) ->
  gen_server:call(list_to_atom(atom_to_list(?LOG_INDEX_DAEMON)++integer_to_list(Partition)), {get_continuation, {Key, Timestamp}}).

add_to_index(Key, Timestamp, Continuation, Partition) ->
  gen_server:call(list_to_atom(atom_to_list(?LOG_INDEX_DAEMON)++integer_to_list(Partition)), {index, {Key, Timestamp}, Continuation}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(IndexIdentier, Partition) ->
  gen_server:start_link({local, list_to_atom(atom_to_list(?LOG_INDEX_DAEMON)++integer_to_list(Partition))}, ?MODULE, {IndexIdentier, Partition}, []).

init({IndexIdentifier, Partition}) ->
  logger:notice(#{
    action => "Starting log index daemon",
    registered_as => ?MODULE,
    pid => self()
  }),
  IndexIdentifierForPartition = list_to_atom(atom_to_list(IndexIdentifier)++integer_to_list(Partition)),
  ets:new(IndexIdentifierForPartition, [ordered_set, named_table, ?TABLE_CONCURRENCY]),
  {ok, #log_index_daemon_state{indexidentifier = IndexIdentifierForPartition}}.

handle_call({get_continuation, {Key, Timestamp}}, _From, State = #log_index_daemon_state{}) ->
  Continuation =
    case ets:lookup(State#log_index_daemon_state.indexidentifier, {Key, Timestamp}) of
      [] -> start;
      [{_Key, Cont}] -> Cont
    end,
  {reply, {ok, Continuation} , State};

handle_call({index, {Key, Timestamp}, Continuation}, _From, State = #log_index_daemon_state{}) ->
  Result = ets:insert_new(State#log_index_daemon_state.indexidentifier, {{Key, Timestamp}, Continuation}),
  {reply, {ok, Result}, State}.

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
