-module(simple_tests).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

start() ->
    os:putenv("reset_log", "true"),
    logger:set_primary_config(#{level => info}),
    {ok, GingkoSup} = gingko_sup:start_link(),
    GingkoSup.

stop(Sup) ->
    exit(Sup, normal),
    Ref = monitor(process, Sup),
    receive
        {'DOWN', Ref, process, Sup, _Reason} ->
            logger:info("Gingko shutdown successful"),
            ok
    after 1000 ->
        error(exit_timeout)
    end.


fixture_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun transaction_abort_read_test/1,
            fun write_read_witnout_clock_test/1,
            fun counter_write_multiOP_test/1,
            fun counter_write_singleOP_test/1,
            %%fun cache_invalidation_test_single_object/1,
            %%fun cache_invalidation_test_multi_object/1,
            fun writeupdate_test/1,
            fun write_and_commit_test/1,
            fun clock_cache_miss_test/1,
            fun clock_cache_hit_test/1,
            fun forced_cache_invalidation_test/1
        ]
    }.

transaction_abort_read_test(_Config) ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 45,
    gingko:update(counter_aborted, Type, TransactionId, DownstreamOp),
    gingko:abort([counter_aborted], TransactionId),
    {ok, Data} = gingko:get_version(counter_aborted, Type),
    ?_assertEqual({counter_aborted,Type,0}, Data).


write_read_witnout_clock_test(_Config) ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 45,
    gingko:update(counter_single, Type, TransactionId, DownstreamOp),
    gingko:commit([counter_single], TransactionId, {1, 1234}, vectorclock:new()),
    {ok, Data} = gingko:get_version(counter_single, Type),
    ?_assertEqual({counter_single,Type,45}, Data).

counter_write_singleOP_test(_Config)->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 45,
    gingko:update(counter_single, Type, TransactionId, DownstreamOp),
    gingko:commit([counter_single], TransactionId, {1, 1234}, vectorclock:new()),
    %% gingko:commit([counter_single], arbitrary_txid, {1, 1234}, vectorclock:new()).
    {ok, Data} = gingko:get_version(counter_single, Type, vectorclock:new()),
    %% gingko:get_version(counter_single, antidote_crdt_counter_pn, vectorclock:new()).
    ?_assertEqual({counter_single,Type,45}, Data).

counter_write_multiOP_test(_Config)->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 1,
    lists:map(fun(_Index) ->  gingko:update(counter_multi, Type, TransactionId, DownstreamOp) end,lists:seq(1,5)),
    gingko:commit([counter_multi], TransactionId, {1, 1234}, vectorclock:new()),
    {ok, Data} = gingko:get_version(counter_multi, Type, vectorclock:new()),
    ?_assertEqual({counter_multi,Type,5},Data).

write_and_commit_test(_Config) ->
    TransactionId = dummy_txID,
    Type = antidote_crdt_register_mv,
    DownstreamOp = {testMV,<<"b">>, []},

    %% gingko:update(mvKey, antidote_crdt_register_mv, dummy_txID,  {testMV,<<"b">>, []}).
    gingko:update(mvKey, Type, TransactionId, DownstreamOp),
    gingko:commit([mvKey], TransactionId, {1, 1234}, vectorclock:new()),
    %% gingko:commit([mvKey],dummy_txID,{1, 1234},vectorclock:new()).

    {ok, Data} = gingko:get_version(mvKey, Type, vectorclock:new()),
    ?_assertEqual({mvKey,Type,[{testMV,<<"b">>}]},Data).


%% updated but not committed operations result in empty version
writeupdate_test(_Config) ->
    Type = antidote_crdt_register_mv,
    DownstreamOp = {1, 1, []},
    DownstreamOp2 = {1, 1, []},

    gingko:update(b, Type, 1, DownstreamOp),
    gingko:update(b, Type, 2, DownstreamOp2),

    {ok, Data} = gingko:get_version(b, Type, vectorclock:new()),
    ?_assertEqual({b,Type,[]},Data).



cache_invalidation_test_single_object(_Config) ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 10,
    gingko:update(counter_single, Type, TransactionId, DownstreamOp),
    gingko:commit([counter_single], TransactionId, {1, 1234}, vectorclock:new()),
    {ok, Data0} = gingko:get_version(counter_single, Type, vectorclock:new()),
    ?_assertEqual({counter_single,Type,10},Data0),
    gingko:update(counter_single, Type, TransactionId, DownstreamOp),
    gingko:update(counter_single, Type, TransactionId, DownstreamOp),
    {ok, Data1} = gingko:get_version(counter_single, Type, vectorclock:new()),
    ?_assertEqual({counter_single,Type,10},Data1), %% Operation not committed yet so the updates are not visible.
    gingko:commit([counter_single], TransactionId, {1, 1234}, vectorclock:new()),
    {ok, Data2} = gingko:get_version(counter_single, Type, vectorclock:new()),
    ?_assertEqual({counter_single,Type,30},Data2).

cache_invalidation_test_multi_object(_Config) ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 10,
    gingko:update(counter_single1, Type, TransactionId, DownstreamOp),
    gingko:commit([counter_single1], TransactionId, {1, 1234}, vectorclock:new()),
    {ok, Data0} = gingko:get_version(counter_single1, Type, vectorclock:new()),
    ?_assertEqual({counter_single1,Type,10},Data0),
    gingko:update(counter_single1, Type, TransactionId, DownstreamOp),
    gingko:update(counter_single2, Type, TransactionId, DownstreamOp),
    {ok, Data1} = gingko:get_version(counter_single1, Type, vectorclock:new()),
    ?_assertEqual({counter_single1,Type,10},Data1), %% Operation not committed yet so the updates are not visible.
    gingko:commit([counter_single1,counter_single2], TransactionId, {1, 1234}, vectorclock:new()),
    {ok, Data2} = gingko:get_version(counter_single1, Type, vectorclock:new()),
    {ok, Data3} = gingko:get_version(counter_single2, Type, vectorclock:new()),
    ?_assertEqual({counter_single1,Type,20},Data2),
    ?_assertEqual({counter_single2,Type,10},Data3).


clock_cache_miss_test(_Config) ->
    fun() ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 10,
    lists:foreach(fun(ClockValue) ->
        UpdatedClock = vectorclock:set_clock_of_dc(mydc, ClockValue,vectorclock:new()),
        gingko:update(clock_counter_single, Type, TransactionId, DownstreamOp),
        gingko:commit([clock_counter_single], TransactionId, {1, 1234}, UpdatedClock),
        {ok,Data} = gingko:get_version(clock_counter_single, Type, UpdatedClock),
        ?assertEqual({clock_counter_single,Type, 10*ClockValue}, Data)
      end, lists:seq(1,20))
end.

clock_cache_hit_test(_Config) ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 10,
    %% First update and red brings the object into the cache.
    gingko:update(clock_counter_single, Type, TransactionId, DownstreamOp),
    gingko:commit([clock_counter_single], TransactionId, {1, 1234}, vectorclock:set_clock_of_dc(mydc, 1,vectorclock:new())),
    {ok,Data1} = gingko:get_version(clock_counter_single, Type, vectorclock:new()),
    %% Second update creates a new version but the old version is still compatible with the timestamp so no re-materialization.
    gingko:update(clock_counter_single, Type, TransactionId, DownstreamOp),
    gingko:commit([clock_counter_single], TransactionId, {1, 1234}, vectorclock:set_clock_of_dc(mydc, 2,vectorclock:new())),
    {ok,Data2} = gingko:get_version(clock_counter_single, Type, vectorclock:new()),

    ?_assertEqual({clock_counter_single,Type, 10}, Data1),
    ?_assertEqual({clock_counter_single,Type, 10}, Data2).

forced_cache_invalidation_test(_Config) ->
    fun() ->
    TransactionId = arbitrary_txid,
    Type = antidote_crdt_counter_pn,
    DownstreamOp = 10,
    lists:foreach(fun(ClockValue) ->
        gingko:update(forced_counter_single, Type, TransactionId, DownstreamOp),
        gingko:commit([forced_counter_single], TransactionId, {1, 1234}, vectorclock:new()),
        cache_daemon:invalidate_cache_objects([forced_counter_single]),
        {ok,Data} = gingko:get_version(forced_counter_single, Type, vectorclock:new()),
        ?assertEqual({forced_counter_single,Type, 10*ClockValue}, Data)
                  end, lists:seq(1,20))
    end.


    