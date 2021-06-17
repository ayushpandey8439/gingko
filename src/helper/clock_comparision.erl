%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Jun 2021 13:48
%%%-------------------------------------------------------------------
-module(clock_comparision).
-author("pandey").

%% API
-export([check_max_time_lt/2,check_max_time_le/2, check_min_time_gt/2, check_min_time_ge/2]).



-spec check_min_time_gt(vectorclock:vectorclock(),vectorclock:vectorclock() | ignore) -> boolean().
check_min_time_gt(SnapshotTime, MinSnapshotTime) when SnapshotTime =/= ignore->
  ((MinSnapshotTime == ignore) orelse (vectorclock:gt(SnapshotTime, MinSnapshotTime)));
check_min_time_gt(SnapshotTime, MinSnapshotTime) when SnapshotTime == ignore->
  false.

-spec check_min_time_ge(vectorclock:vectorclock(),vectorclock:vectorclock() | ignore) -> boolean().
check_min_time_ge(SnapshotTime, MinSnapshotTime) ->
  ((MinSnapshotTime == ignore) orelse (vectorclock:ge(SnapshotTime, MinSnapshotTime))).


-spec check_max_time_lt(vectorclock:vectorclock(),vectorclock:vectorclock() | ignore) -> boolean().
check_max_time_lt(SnapshotTime, MaxSnapshotTime) ->
  ((MaxSnapshotTime == ignore) orelse (vectorclock:lt(SnapshotTime, MaxSnapshotTime))).

-spec check_max_time_le(vectorclock:vectorclock(),vectorclock:vectorclock() | ignore) -> boolean().
check_max_time_le(SnapshotTime, MaxSnapshotTime) when SnapshotTime =/= ignore ->
  ((MaxSnapshotTime == ignore) orelse (vectorclock:le(SnapshotTime, MaxSnapshotTime)));
check_max_time_le(SnapshotTime, MaxSnapshotTime) when SnapshotTime == ignore ->
  false.