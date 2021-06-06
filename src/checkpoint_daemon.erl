%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(checkpoint_daemon).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(checkpoint_daemon_server_state, {checkpointStoreIdentifier}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(CheckpointStoreIdentifier) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, CheckpointStoreIdentifier, []).

init(CheckpointStoreIdentifier) ->
  logger:notice(#{
    action => "Starting Checkpoint Daemon",
    registered_as => ?MODULE,
    pid => self()
  }),
  {ok,CheckpointStore} = dets:open_file(CheckpointStoreIdentifier,[{auto_save, 10000},{keypos,2},{type,set}]),
  {ok, #checkpoint_daemon_server_state{checkpointStoreIdentifier = CheckpointStore}}.

handle_call(_Request, _From, State = #checkpoint_daemon_server_state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #checkpoint_daemon_server_state{}) ->
  {noreply, State}.

handle_info(_Info, State = #checkpoint_daemon_server_state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #checkpoint_daemon_server_state{}) ->
  ok.

code_change(_OldVsn, State = #checkpoint_daemon_server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
