%% @doc gingko sync server
%% @hidden
-module(gingko_sync_server).

-behaviour(gen_server).


-record(state, {
  % open references to logs to be closed after termination
  logs_to_close,

  % local log name
  log_file :: atom()
}).

%% API
-export([start_link/2]).
-export([log_dir_base/1]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2, code_change/3]).


%% @doc Starts the log sync timing server for given node
-spec start_link(node(), integer()) -> {ok, pid()}.
start_link(LogFile, Partition) ->
  gen_server:start_link({global, ?MODULE_STRING++integer_to_list(Partition)}, ?MODULE, {LogFile}, []).


%% @doc Initializes the internal server state
init({LogFile}) ->
  logger:notice(#{
    action => "Starting log sync server",
    registered_as => ?MODULE,
    name => LogFile
  }),

  reset_if_flag_set(LogFile),
  {ok, #state{ logs_to_close = sets:new(), log_file = LogFile }}.


terminate(_Reason, State) ->
  logger:debug(#{
    action => "Shutdown log sync server",
    name => State#state.log_file
  }),

  % close all references to open logs
  CloseLog = fun(LogRef, _) -> disk_log:close(LogRef) end,
  sets:fold(CloseLog, void, State#state.logs_to_close),
  ok.


%% @doc opens the log given by the server name (second argument) and the target node (third argument)
handle_call({get_log, LogFile}, _From, State) ->
  logger:debug(#{
    action => "Open log",
    log => LogFile
  }),

  {ok, Log} = open_log(LogFile),
  {reply, {ok, Log}, State}.


%% @doc Receives a syncing request. Depending on the strategy may or may not sync immediately
handle_cast({sync_log, LogName, ReplyTo}, State) ->
  Log = open_log(LogName),

  logger:debug(#{
    action => "Sync log to disk",
    log => State#state.log_file
  }),
  disk_log:sync(Log),

  ReplyTo ! log_persisted,
  {noreply, State}.


handle_info(Msg, State) ->
  logger:warning(#{ warning => "Unexpected Message", log => State#state.log_file, message => Msg }),
  {noreply, State}.


code_change(_OldVsn, _State, _Extra) ->
  erlang:error(not_implemented).


%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

%% @doc ensures directory where the log is expected and opens the log file.
%%      Recovers if required, logging found terms and bad bytes
open_log(LogFileName) ->
  logger:debug("Trying to open log ~p",[LogFileName]),
  filelib:ensure_dir(log_dir_base(LogFileName)),

  LogFile = log_dir_base(LogFileName) ++ "OP_LOG",

  LogOptions = [{name, LogFile}, {file, LogFile}],
  case disk_log:open(LogOptions) of
    {ok, Name} -> {ok, Name};
    {repaired, Name, {recovered, Rec}, {badbytes, Bad}} ->
      logger:warning("Recovered bad log file, found ~p terms, ~p bad bytes", [Rec, Bad]),
      {ok, Name}
  end.


%% @doc Returns the base log dir path
-spec log_dir_base(node() | string()) -> string().
log_dir_base(LogName) when is_atom(LogName)->
  log_dir_base(atom_to_list(LogName));
log_dir_base(LogName) ->
  % read path
  EnvLogDir = os:getenv("log_root"),
  case EnvLogDir of
    false -> LogDir = "data/op_log/"; % default value if not set
    LogDir -> LogDir
  end,
  LogDir ++ LogName ++ "/".


reset_if_flag_set(LogName) ->
  ResetLogFile = os:getenv("reset_log", "false"),
  case ResetLogFile of
    "true" ->
      LogFile = log_dir_base(LogName) ++ "OP_LOG",
      logger:notice(#{
        action => "Reset log file",
        log => LogName,
        path => LogFile
      }),
      file:delete(LogFile)
    ;
    _ -> ok
  end.
