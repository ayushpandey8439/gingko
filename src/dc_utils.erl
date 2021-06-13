%%%-------------------------------------------------------------------
%%% @author pandey
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. May 2021 01:43
%%%-------------------------------------------------------------------
-module(dc_utils).
-author("pandey").

%% API
-export([now/1]).

-spec now('micro_seconds' | 'milli_seconds') -> non_neg_integer().
now(Granularity) when Granularity == micro_seconds->
  erlang:system_time(Granularity);
now(Granularity) when Granularity == milli_seconds ->
  now(micro_seconds) div 1000.



