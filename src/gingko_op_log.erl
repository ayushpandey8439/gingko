%% @doc The operation log which receives requests and manages recovery of log files.
%% @hidden
-module(gingko_op_log).
-include("gingko.hrl").

-behaviour(gen_server).

%% TODO
-type server() :: any().
-type log() :: any().
-type log_entry() :: #log_record{}.
-type gen_from() :: any().

-export([start_link/3]).
-export([append/3, read_log_entries/3]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2,  handle_info/2]).

%% ==============
%% API
%% ==============

%% @doc Appends a log entry to the end of the log.
%%
%% When the function returns 'ok' the entry is guaranteed to be persistently stored.
%%
%% @param Log the process returned by start_link.
%% @param Entry the log record to append.
-spec append(atom(), log_entry(), integer()) -> ok  | {error, Reason :: term()}.
append(Key, Entry, Partition) ->
  case gen_server:call(list_to_atom(atom_to_list(?LOGGING_MASTER)++integer_to_list(Partition)), {add_log_entry,Key, Entry}) of
    %% request got stuck in queue (server busy) and got retry signal
    retry -> logger:debug("Retrying request"), append(Key, Entry, Partition);
    Reply ->  Reply
  end.
%% @doc Read all log entries belonging to the given log and in a certain range with a custom accumulator.
%%
%% The function works similar to lists:foldl for reading the entries.
%%
%% Returns the accumulator value after reading all matching log entries.
%% @param Log the process returned by start_link.
%% @param FirstIndex start reading from this index on, inclusive
%% @param LastIndex stop at this index, inclusive
%% @param FoldFunction function that takes a single log entry and the current accumulator and returns the new accumulator
%% @param Starting accumulator
-spec read_log_entries(atom(), continuation() | start, integer()) -> {ok, [#log_read{}]}.
read_log_entries(Key, Continuation, Partition) ->
  case gen_server:call(list_to_atom(atom_to_list(?LOGGING_MASTER)++integer_to_list(Partition)), {read_log_entries, Key, Continuation, Partition}) of
    retry -> logger:debug("Retrying request"), read_log_entries(Key, Continuation, Partition);
    Reply -> Reply
  end.

%%%===================================================================
%%% State
%%%===================================================================

-record(state, {
  % recovery related state
  % receiver of the recovered log messages
  recovery_receiver :: pid(),
  % if recovering reply with busy status to requests
  recovering :: true | false,

  % log name, used for storing logs in a directory related to the name
  log_name :: atom(),
  % requests waiting until log is not recovering anymore or missing indices have been updated
  waiting_for_reply = [] :: [any()],
  % handles syncing and opening the log
  sync_server :: pid(),

  % stores current writable index
  next_index :: integer(),

  % Index storing the LSN with the continuation from which the read has to start.
  % This will affect the filtering being done in line 250.

  current_continuation:: continuation()

}).


% log starts with this default index
-define(STARTING_INDEX, 0).


%% @doc Starts the op log server for given server name and recovery receiver process
-spec start_link(term(), pid() | none, integer()) -> {ok, pid()}.
start_link(LogName, RecoveryReceiver, Partition) ->
  ProcessName = list_to_atom(atom_to_list(?LOGGING_MASTER)++integer_to_list(Partition)),
  LogFile =  list_to_atom(LogName++integer_to_list(Partition)),
  gen_server:start_link({local, ProcessName}, ?MODULE, {LogFile, RecoveryReceiver, Partition}, []).


%% @doc Initializes the internal server state
-spec init({node(), pid()}) -> {ok, #state{}}.
init({LogFile, RecoveryReceiver, Partition}) ->
  logger:info(#{
    action => "Starting op log server",
    registered_as => list_to_atom(atom_to_list(?LOGGING_MASTER)++integer_to_list(Partition)),
    name => LogFile,
    receiver => RecoveryReceiver
  }),

  case RecoveryReceiver of
    none -> ActualReceiver =
      spawn(fun Loop() ->
        receive Message -> logger:notice("Received dummy message: ~p",[Message]) end,
        Loop()
      end);
    _ -> ActualReceiver = RecoveryReceiver
  end,


  {ok, SyncServer} = gingko_sync_server:start_link(LogFile, Partition),

  gen_server:cast(self(), start_recovery),
  {ok, #state{
    log_name = LogFile,
    recovery_receiver = ActualReceiver,
    recovering = true,
    sync_server = SyncServer,
    next_index = ?STARTING_INDEX,
    current_continuation = start
  }}.


%% ------------------
%% ASYNC LOG RECOVERY
%% ------------------

%% @doc Either
%% 1) starts async recovery and does not reply until recovery is finished
%% or
%% 2) finishes async recovery and replies to waiting processes
-spec handle_cast
    (start_recovery, #state{}) -> {noreply, #state{}};          %1)
    ({finish_recovery, #{}}, #state{}) -> {noreply, #state{}}.  %2)
handle_cast(start_recovery, State) when State#state.recovering == true ->
  LogName = State#state.log_name,
  Receiver = State#state.recovery_receiver,
  LogServer = State#state.sync_server,
  logger:notice("[~p] Async recovery started", [LogName]),

  GenServer = self(),
  AsyncRecovery = fun() ->
    NextIndex = recover_all_logs(LogName, Receiver, LogServer),
    %% TODO recovery
    gen_server:cast(GenServer, {finish_recovery, NextIndex})
                  end,
  spawn_link(AsyncRecovery),
  {noreply, State};

handle_cast({finish_recovery, NextIndexMap}, State) ->
  % reply to waiting processes to try their requests again
  reply_retry_to_waiting(State#state.waiting_for_reply),
  % save write-able index map and finish recovery
  logger:info("[~p] Recovery process finished", [State#state.log_name]),
  {noreply, State#state{recovering = false, next_index = NextIndexMap, waiting_for_reply = []}};

handle_cast(Msg, State) ->
  logger:warning("[~p] Swallowing unexpected message: ~p", [State#state.log_name, Msg]),
  {noreply, State}.


terminate(_Reason, State) ->
  gen_server:stop(State#state.sync_server),
  ok.


%% @doc Either
%% 1) adds a log entry for given node and a {Index, Data} pair
%% or
%% 2) reads the log
-spec handle_call
    ({add_log_entry, atom(), log_entry()}, gen_from(), #state{}) ->
    {noreply, #state{}} | %% if still recovering
    {reply, {error, index_already_written}, #state{}} | %% if index is already written
    {reply, ok, #state{}}; %% entry is persisted

    ({read_log_entries, any(), integer(), integer()}, gen_from(), #state{}) ->
    {noreply, #state{}} | %% if still recovering or index for given node behind
    {reply, {ok, [#log_read{}]}, #state{}}. %% accumulated entries

handle_call({add_log_entry, _Data}, From, State) when State#state.recovering == true ->
  logger:debug("[~p] Waiting for recovery: ~p", [State#state.log_name, From]),
  Waiting = State#state.waiting_for_reply,
  {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};

handle_call({add_log_entry, Key, LogEntry}, From, State) ->

  NextIndex = State#state.next_index,
  LogName = State#state.log_name,
  LogServer = State#state.sync_server,
  Waiting = State#state.waiting_for_reply,

  {ok, Log} = gen_server:call(LogServer, {get_log, LogName}),
  logger:debug(#{
    action => "Logging",
    log => Log,
    index => NextIndex,
    data => LogEntry
  }),

 % Index_Continuation = case disk_log:chunk(Log, start, infinity) of
  %  {Continuation, _Terms} ->
 %     Continuation;
 %   {Continuation, _Terms, _BadBytes} ->
 %     Continuation;
  %  _ ->
  %    start
  %end,
  %TODO Send the key to the log indexer and insert the first entry for the chunk into the index.
  %log_index_daemon:add_to_index(Key, ignore, Index_Continuation),

  ok = disk_log:alog(Log, {NextIndex, LogEntry}),

  % wait for sync reply
  gen_server:cast(LogServer, {sync_log, LogName, self()}),
  receive log_persisted -> ok end,

  logger:debug("[~p] Log entry at ~p persisted",
    [State#state.log_name, NextIndex]),

  % index of another request may be up to date, send retry messages
  reply_retry_to_waiting(Waiting),
  {reply, {ok, NextIndex}, State#state{
    % increase index counter for node by one
    next_index = NextIndex + 1,
    % empty waiting queue
    waiting_for_reply = []
  }};


handle_call(_Request, From, State)
  when State#state.recovering == true ->
  logger:debug("[~p] Read, waiting for recovery", [State#state.log_name]),
  Waiting = State#state.waiting_for_reply,
  {noreply, State#state{ waiting_for_reply = Waiting ++ [From] }};

handle_call({read_log_entries,Key, Continuation, Partition}, _From, State) ->
  LogName = State#state.log_name,
  LogServer = State#state.sync_server,
  Waiting = State#state.waiting_for_reply,

  {ok, Log} = gen_server:call(LogServer, {get_log, LogName},100000),
  Terms = read_continuations(Log, [], Continuation),

  reply_retry_to_waiting(Waiting),
  {reply, {ok, Terms}, State#state{waiting_for_reply = []}}.


handle_info(Msg, State) ->
  logger:warning("Swallowing unexpected message: ~p", [Msg]),
  {noreply, State}.




%%%===================================================================
%%% Private Functions Implementation
%%%===================================================================

%% @doc Replies a 'retry' message to all waiting process to retry their action again
-spec reply_retry_to_waiting([gen_from()]) -> ok.
reply_retry_to_waiting(WaitingProcesses) ->
  Reply = fun(Process, _) -> gen_server:reply(Process, retry) end,
  lists:foldl(Reply, void, WaitingProcesses),
  ok.


%% @doc recovers all logs for given server name
%%      the server name should be the local one
%%      also ensures that the directory actually exists
%%
%% sends pid() ! {log_recovery, Node, {Index, Data}}
%%      for each entry in one log
%%
%% sends pid() ! {log_recovery_done}
%%      once after processing finished
-spec recover_all_logs(server(), pid(), pid()) -> any().
%%noinspection ErlangUnboundVariable
recover_all_logs(LogName, Receiver, LogServer) when is_atom(LogName) ->
  recover_all_logs(atom_to_list(LogName), Receiver, LogServer);
recover_all_logs(LogName, Receiver, LogServer) ->
  % make sure the folder exists
  LogPath = gingko_sync_server:log_dir_base(LogName),
  filelib:ensure_dir(LogPath),

  ProcessLogFile = fun(LogFile, Index) ->
    logger:debug(#{
      action => "Recovering logfile",
      log => LogName,
      file => LogFile
    }),

    {ok, Log} = gen_server:call(LogServer, {get_log, LogName}),

    % read all terms
    Terms = read_all(Log),

    % For each entry {log_recovery, {Index, Data}} is sent
    SendTerm = fun({LogIndex, Data}, _) -> Receiver ! {log_recovery, {LogIndex, Data}} end,
    lists:foldl(SendTerm, void, Terms),

    case Terms of
      [] -> LastIndex = 0;
      _ -> {LastIndex, _} = hd(lists:reverse(Terms))
    end,

    case Index =< LastIndex of
      true -> logger:debug("Jumping from ~p to ~p index", [Index, LastIndex]);
      _ -> logger:emergency("Index corrupt! ~p to ~p jump found", [Index, LastIndex])
    end,

    LastIndex
  end,

  {ok, LogFiles} = file:list_dir(LogPath),

  % accumulate node -> next free index
  IndexAcc = 0,

  LastIndex = lists:foldl(ProcessLogFile, IndexAcc, LogFiles),
  logger:debug("Receiver: ~p", [Receiver]),

  Receiver ! log_recovery_done,

  LastIndex.


%% @doc reads all terms from given log
-spec read_all(log()) -> [term()].
read_all(Log) ->
  read_all(Log, [], start).


read_all(Log, Terms, Cont) ->
  case disk_log:chunk(Log, Cont) of
    eof -> Terms;
    {Cont2, ReadTerms} -> read_all(Log, Terms ++ ReadTerms, Cont2)
  end.


%% @doc reads terms from given log starting with a specific continuation and also returns the continuations with the read log entries.
-spec read_continuations(log(), [#log_read{}], continuation()) -> [#log_read{}].
read_continuations(Log, Terms, Cont) ->
  case disk_log:chunk(Log, Cont) of
    eof -> Terms;
    {Cont2, ReadTerms} -> read_continuations(Log, Terms ++ [#log_read{log_entry = ReadTerm, continuation = Cont} || ReadTerm <- ReadTerms], Cont2)
  end.